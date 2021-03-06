use pg_sys::*;
use pgx::*;
use std::collections::HashMap;
use std::ffi::CStr;

// https://www.postgresql.org/docs/13/fdw-callbacks.html
pub type Tuple = (String, Option<pg_sys::Datum>, pgx::PgOid);
pub type FdwOption = HashMap<String, String>;

#[derive(Debug)]
pub struct FdwOptions {
    pub server_opts: FdwOption,
    pub table_opts: FdwOption,
    pub table_name: String,
    pub table_namespace: String,
}

impl FdwOptions {
    pub fn from_relation(relation: &PgRelation) -> Self {
        let table = PgBox::<pg_sys::ForeignTable>::from_pg(unsafe {
            pg_sys::GetForeignTable(relation.rd_id)
        });
        let server = PgBox::<pg_sys::ForeignServer>::from_pg(unsafe {
            pg_sys::GetForeignServer(table.serverid)
        });

        Self {
            server_opts: Self::from_pg_list(server.options),
            table_opts: Self::from_pg_list(table.options),
            table_name: relation.name().into(),
            table_namespace: relation.namespace().into(),
        }
    }

    fn from_pg_list(opts: *mut pg_sys::List) -> FdwOption {
        if opts.is_null() {
            return HashMap::new();
        }

        let pg_list = PgList::<pg_sys::DefElem>::from_pg(opts);

        pg_list
            .iter_ptr()
            .map(|ptr| unsafe { Self::elem_to_tuple(ptr) })
            .collect::<FdwOption>()
    }

    unsafe fn elem_to_tuple(elem: *mut pg_sys::DefElem) -> (String, String) {
        let key = (*elem).defname;
        let value = (*((*elem).arg as *mut pg_sys::Value)).val.str_;

        match (CStr::from_ptr(key).to_str(), CStr::from_ptr(value).to_str()) {
            (Ok(k), Ok(v)) => (k.into(), v.into()),
            (Err(err), _) => error!("Option list key err {}", err),
            (_, Err(err)) => error!("Option list value err {}", err),
        }
    }
}

pub trait ForeignData {
    type Item: IntoDatum;
    type RowIterator: Iterator<Item = Vec<Self::Item>>;

    fn begin(options: &FdwOptions) -> Self;
    fn execute(&mut self, desc: &PgTupleDesc) -> Self::RowIterator;
    fn indices(_options: &FdwOptions) -> Option<Vec<String>> {
        None
    }

    fn insert(&self, _desc: &PgTupleDesc, _row: Vec<Tuple>) -> Option<Vec<Tuple>> {
        None
    }

    fn update(
        &self,
        _desc: &PgTupleDesc,
        _row: Vec<Tuple>,
        _indices: Vec<Tuple>,
    ) -> Option<Vec<Tuple>> {
        None
    }

    fn delete(&self, _desc: &PgTupleDesc, _indices: Vec<Tuple>) -> Option<Vec<Tuple>> {
        None
    }
}

#[derive(Debug)]
pub struct FdwState<T: ForeignData> {
    state: T,
    itr: *mut T::RowIterator,
}

impl<T: ForeignData> FdwState<T> {
    unsafe extern "C" fn get_foreign_rel_size(
        _root: *mut PlannerInfo,
        baserel: *mut RelOptInfo,
        _foreigntableid: Oid,
    ) {
        (*baserel).rows = 0.0;
    }

    unsafe extern "C" fn get_foreign_paths(
        root: *mut PlannerInfo,
        baserel: *mut RelOptInfo,
        _foreigntableid: Oid,
    ) {
        pg_sys::add_path(
            baserel,
            pg_sys::create_foreignscan_path(
                root,
                baserel,
                std::ptr::null_mut(),
                (*baserel).rows,
                pg_sys::Cost::from(10),
                pg_sys::Cost::from(0),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            ) as *mut pg_sys::Path,
        )
    }

    unsafe extern "C" fn get_foreign_plan(
        _root: *mut PlannerInfo,
        baserel: *mut RelOptInfo,
        _foreigntableid: Oid,
        _best_path: *mut ForeignPath,
        tlist: *mut List,
        scan_clauses: *mut List,
        outer_plan: *mut Plan,
    ) -> *mut ForeignScan {
        let scan_relid = (*baserel).relid;
        let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

        pg_sys::make_foreignscan(
            tlist,
            scan_clauses,
            scan_relid,
            scan_clauses,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            outer_plan,
        )
    }

    extern "C" fn begin_foreign_scan(node: *mut ForeignScanState, _eflags: ::std::os::raw::c_int) {
        let mut fdw_state = PgBox::<Self>::alloc0();
        let mut n = PgBox::<ForeignScanState>::from_pg(node);
        let rel = unsafe { PgRelation::from_pg(n.ss.ss_currentRelation) };
        let opts = FdwOptions::from_relation(&rel);

        fdw_state.state = T::begin(&opts);
        fdw_state.itr = std::ptr::null_mut();

        n.fdw_state = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;
        // (*node).fdw_state = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;
    }

    unsafe extern "C" fn iterate_foreign_scan(node: *mut ForeignScanState) -> *mut TupleTableSlot {
        let mut n = PgBox::<ForeignScanState>::from_pg(node);
        let mut fdw_state = PgBox::<Self>::from_pg(n.fdw_state as *mut Self);
        let mut fdw_itr = PgBox::<T::RowIterator>::from_pg(fdw_state.itr);

        let rel = PgRelation::from_pg(n.ss.ss_currentRelation);

        let tupdesc = PgTupleDesc::from_pg_copy(rel.rd_att);

        let slot = Self::exec_clear_tuple(n.ss.ss_ScanTupleSlot);
        let (item, itr_ptr) = Self::itr_next(&mut fdw_itr, &mut fdw_state, &tupdesc);

        fdw_state.itr = itr_ptr;
        n.fdw_state = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;

        item.map_or(slot, |row| Self::store_tuple(slot, &tupdesc, row))
    }

    fn itr_next(
        fdw_itr: &mut PgBox<<T as ForeignData>::RowIterator>,
        fdw_state: &mut PgBox<FdwState<T>>,
        tupdesc: &PgTupleDesc,
    ) -> (
        Option<Vec<<T as ForeignData>::Item>>,
        *mut <T as ForeignData>::RowIterator,
    ) {
        if fdw_itr.is_null() {
            let mut itr = fdw_state.state.execute(&tupdesc);
            let item = itr.next();
            let itr_ptr = Box::into_raw(Box::new(itr)) as *mut T::RowIterator;

            (item, itr_ptr)
        } else {
            (fdw_itr.next(), fdw_itr.as_ptr())
        }
    }

    fn store_tuple(
        slot: *mut TupleTableSlot,
        tupdesc: &PgTupleDesc,
        row: Vec<<T as ForeignData>::Item>,
    ) -> *mut TupleTableSlot {
        let attrs_len = tupdesc.len();
        let mut nulls = vec![true; attrs_len];
        let mut datums = vec![0 as pg_sys::Datum; attrs_len];
        let mut row_iter = row.into_iter();

        for (i, _attr) in tupdesc.iter().enumerate() {
            if let Some(row_i) = row_iter.next() {
                match row_i.into_datum() {
                    Some(datum) => {
                        datums[i] = datum;
                        nulls[i] = false;
                    }
                    None => continue,
                }
            } else {
                continue;
            }
        }

        unsafe {
            let tuple =
                pg_sys::heap_form_tuple(tupdesc.as_ptr(), datums.as_mut_ptr(), nulls.as_mut_ptr());

            pg_sys::ExecStoreHeapTuple(tuple, slot, false)
        }
    }

    unsafe fn exec_clear_tuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
        if let Some(fun) = (*(*slot).tts_ops).clear {
            fun(slot);
        }

        slot
    }

    unsafe fn get_some_attrs(slot: *mut TupleTableSlot, natts: i32) -> *mut TupleTableSlot {
        if let Some(fun) = (*(*slot).tts_ops).getsomeattrs {
            fun(slot, natts);
        }

        slot
    }

    unsafe extern "C" fn re_scan_foreign_scan(_node: *mut ForeignScanState) {}

    unsafe extern "C" fn end_foreign_scan(_node: *mut ForeignScanState) {}

    unsafe extern "C" fn add_foreign_update_targets(
        parsetree: *mut Query,
        _target_rte: *mut RangeTblEntry,
        target_relation: Relation,
    ) {
        let rel = PgRelation::from_pg(target_relation);
        let opts = FdwOptions::from_relation(&rel);
        let tupdesc = PgTupleDesc::from_pg_copy((*target_relation).rd_att);

        if let Some(keys) = T::indices(&opts) {
            let mut list = PgList::<TargetEntry>::from_pg((*parsetree).targetList);
            tupdesc
                .iter()
                .filter(|attr| keys.contains(&attr.name().into()))
                .for_each(|attr| {
                    let var = pg_sys::makeVar(
                        (*parsetree).resultRelation as pg_sys::Index,
                        attr.attnum,
                        attr.atttypid,
                        attr.atttypmod,
                        attr.attcollation,
                        0,
                    );

                    let ckey = std::ffi::CString::new(attr.name()).unwrap();
                    let tle = pg_sys::makeTargetEntry(
                        var as *mut pg_sys::Expr,
                        (list.len() + 1) as i16,
                        pg_sys::pstrdup(ckey.as_ptr()),
                        true,
                    );

                    list.push(tle);
                });

            (*parsetree).targetList = list.into_pg();
        }
    }

    extern "C" fn begin_foreign_modify(
        _mtstate: *mut ModifyTableState,
        rinfo: *mut ResultRelInfo,
        _fdw_private: *mut List,
        _subplan_index: ::std::os::raw::c_int,
        _eflags: ::std::os::raw::c_int,
    ) {
        let mut fdw_state = PgBox::<Self>::alloc0();
        let mut rinfo_box = PgBox::<ResultRelInfo>::from_pg(rinfo);
        let rel = unsafe { PgRelation::from_pg(rinfo_box.ri_RelationDesc) };

        let opts = FdwOptions::from_relation(&rel);

        fdw_state.state = T::begin(&opts);
        fdw_state.itr = std::ptr::null_mut();

        rinfo_box.ri_FdwState = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;
    }

    extern "C" fn exec_foreign_insert(
        _estate: *mut EState,
        rinfo: *mut ResultRelInfo,
        slot: *mut TupleTableSlot,
        _plan_slot: *mut TupleTableSlot,
    ) -> *mut TupleTableSlot {
        let mut rinfo_box = PgBox::<ResultRelInfo>::from_pg(rinfo);
        let slot_box = PgBox::<TupleTableSlot>::from_pg(slot);
        let fdw_state = PgBox::<Self>::from_pg(rinfo_box.ri_FdwState as *mut Self);
        let tupdesc = PgTupleDesc::from_pg_copy(slot_box.tts_tupleDescriptor);

        let tuples = Self::slot_to_tuples(&slot_box, &tupdesc);

        let _result = fdw_state.state.insert(&tupdesc, tuples);

        rinfo_box.ri_FdwState = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;
        slot_box.into_pg()
    }

    fn slot_to_tuples(slot: &PgBox<TupleTableSlot>, tupdesc: &PgTupleDesc) -> Vec<Tuple> {
        if slot.tts_nvalid == 0 {
            unsafe {
                Self::get_some_attrs(slot.as_ptr(), tupdesc.natts);
            }
        };

        let (datums, nulls) = unsafe {
            (
                std::slice::from_raw_parts(slot.tts_values, slot.tts_nvalid as usize),
                std::slice::from_raw_parts(slot.tts_isnull, (*slot).tts_nvalid as usize),
            )
        };

        let tuples: Vec<Tuple> = tupdesc
            .iter()
            .enumerate()
            .map(|(i, attr)| {
                let oid = attr.type_oid();
                (
                    attr.name().into(),
                    unsafe {
                        pg_sys::Datum::from_datum(
                            datums[i].to_owned(),
                            nulls[i].to_owned(),
                            oid.value(),
                        )
                    },
                    oid,
                )
            })
            .collect();

        tuples
    }

    extern "C" fn exec_foreign_update(
        _estate: *mut EState,
        rinfo: *mut ResultRelInfo,
        slot: *mut TupleTableSlot,
        plan_slot: *mut TupleTableSlot,
    ) -> *mut TupleTableSlot {
        let mut rinfo_box = PgBox::<ResultRelInfo>::from_pg(rinfo);
        let fdw_state = PgBox::<Self>::from_pg(rinfo_box.ri_FdwState as *mut Self);
        let slot_box = PgBox::<TupleTableSlot>::from_pg(slot);
        let plan_slot_box = PgBox::<TupleTableSlot>::from_pg(plan_slot);

        let tupdesc = PgTupleDesc::from_pg_copy(slot_box.tts_tupleDescriptor);
        let plan_tupdesc = PgTupleDesc::from_pg_copy(plan_slot_box.tts_tupleDescriptor);

        let tuples = Self::slot_to_tuples(&slot_box, &tupdesc);
        let indices = Self::slot_to_tuples(&plan_slot_box, &plan_tupdesc);

        let _result = fdw_state.state.update(&tupdesc, tuples, indices);

        rinfo_box.ri_FdwState = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;
        slot_box.into_pg()
    }

    extern "C" fn exec_foreign_delete(
        _estate: *mut EState,
        rinfo: *mut ResultRelInfo,
        slot: *mut TupleTableSlot,
        plan_slot: *mut TupleTableSlot,
    ) -> *mut TupleTableSlot {
        let mut rinfo_box = PgBox::<ResultRelInfo>::from_pg(rinfo);
        let fdw_state = PgBox::<Self>::from_pg(rinfo_box.ri_FdwState as *mut Self);
        let plan_slot_box = PgBox::<TupleTableSlot>::from_pg(plan_slot);

        let tupdesc = PgTupleDesc::from_pg_copy(plan_slot_box.tts_tupleDescriptor);

        let tuples = Self::slot_to_tuples(&plan_slot_box, &tupdesc);
        let _result = fdw_state.state.delete(&tupdesc, tuples);

        rinfo_box.ri_FdwState = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;

        slot
    }

    extern "C" fn end_foreign_modify(_estate: *mut EState, _rinfo: *mut ResultRelInfo) {}

    pub fn into_datum() -> pg_sys::Datum {
        let mut handler = PgBox::<pg_sys::FdwRoutine>::alloc_node(pg_sys::NodeTag_T_FdwRoutine);

        handler.GetForeignRelSize = Some(Self::get_foreign_rel_size);
        handler.GetForeignPaths = Some(Self::get_foreign_paths);
        handler.GetForeignPlan = Some(Self::get_foreign_plan);
        handler.BeginForeignScan = Some(Self::begin_foreign_scan);
        handler.IterateForeignScan = Some(Self::iterate_foreign_scan);
        handler.ReScanForeignScan = Some(Self::re_scan_foreign_scan);
        handler.EndForeignScan = Some(Self::end_foreign_scan);
        handler.EndForeignInsert = None;
        handler.ReparameterizeForeignPathByChild = None;
        handler.ShutdownForeignScan = None;
        handler.ReInitializeDSMForeignScan = None;
        handler.GetForeignJoinPaths = None;
        handler.GetForeignUpperPaths = None;
        handler.AddForeignUpdateTargets = Some(Self::add_foreign_update_targets);
        handler.PlanForeignModify = None;
        handler.BeginForeignModify = Some(Self::begin_foreign_modify);
        handler.ExecForeignInsert = Some(Self::exec_foreign_insert);
        handler.ExecForeignUpdate = Some(Self::exec_foreign_update);
        handler.ExecForeignDelete = Some(Self::exec_foreign_delete);
        handler.EndForeignModify = Some(Self::end_foreign_modify);
        handler.IsForeignRelUpdatable = None;
        handler.PlanDirectModify = None;
        handler.BeginDirectModify = None;
        handler.IterateDirectModify = None;
        handler.EndDirectModify = None;
        handler.GetForeignRowMarkType = None;
        handler.RefetchForeignRow = None;
        handler.RecheckForeignScan = None;
        handler.ExplainForeignScan = None;
        handler.ExplainForeignModify = None;
        handler.ExplainDirectModify = None;
        handler.AnalyzeForeignTable = None;
        handler.ImportForeignSchema = None;
        handler.IsForeignScanParallelSafe = None;
        handler.EstimateDSMForeignScan = None;
        handler.InitializeDSMForeignScan = None;
        handler.InitializeWorkerForeignScan = None;

        return handler.into_pg() as pg_sys::Datum;
    }
}

//GetForeignJoinPaths_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        root: *mut PlannerInfo,
//        joinrel: *mut RelOptInfo,
//        outerrel: *mut RelOptInfo,
//        innerrel: *mut RelOptInfo,
//        jointype: JoinType,
//        extra: *mut JoinPathExtraData,
//    ),
//>;
//GetForeignUpperPaths_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        root: *mut PlannerInfo,
//        stage: UpperRelationKind,
//        input_rel: *mut RelOptInfo,
//        output_rel: *mut RelOptInfo,
//        extra: *mut ::std::os::raw::c_void,
//    ),
//>;
//AddForeignUpdateTargets_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        parsetree: *mut Query,
//        target_rte: *mut RangeTblEntry,
//        target_relation: Relation,
//    ),
//>;
//PlanForeignModify_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        root: *mut PlannerInfo,
//        plan: *mut ModifyTable,
//        resultRelation: Index,
//        subplan_index: ::std::os::raw::c_int,
//    ) -> *mut List,
//>;
//BeginForeignModify_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        mtstate: *mut ModifyTableState,
//        rinfo: *mut ResultRelInfo,
//        fdw_private: *mut List,
//        subplan_index: ::std::os::raw::c_int,
//        eflags: ::std::os::raw::c_int,
//    ),
//>;
//ExecForeignUpdate_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        estate: *mut EState,
//        rinfo: *mut ResultRelInfo,
//        slot: *mut TupleTableSlot,
//        planSlot: *mut TupleTableSlot,
//    ) -> *mut TupleTableSlot,
//>;
//BeginForeignInsert_function = ::std::option::Option<
//    unsafe extern "C" fn(mtstate: *mut ModifyTableState, rinfo: *mut ResultRelInfo),
//>;
//EndForeignInsert_function =
//    ::std::option::Option<unsafe extern "C" fn(estate: *mut EState, rinfo: *mut ResultRelInfo)>;
//IsForeignRelUpdatable_function =
//    ::std::option::Option<unsafe extern "C" fn(rel: Relation) -> ::std::os::raw::c_int>;
//PlanDirectModify_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        root: *mut PlannerInfo,
//        plan: *mut ModifyTable,
//        resultRelation: Index,
//        subplan_index: ::std::os::raw::c_int,
//    ) -> bool,
//>;
//BeginDirectModify_function = ::std::option::Option<
//    unsafe extern "C" fn(node: *mut ForeignScanState, eflags: ::std::os::raw::c_int),
//>;
//IterateDirectModify_function =
//    ::std::option::Option<unsafe extern "C" fn(node: *mut ForeignScanState) -> *mut TupleTableSlot>;
//EndDirectModify_function =
//    ::std::option::Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;
//GetForeignRowMarkType_function = ::std::option::Option<
//    unsafe extern "C" fn(rte: *mut RangeTblEntry, strength: LockClauseStrength) -> RowMarkType,
//>;
//RefetchForeignRow_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        estate: *mut EState,
//        erm: *mut ExecRowMark,
//        rowid: Datum,
//        slot: *mut TupleTableSlot,
//        updated: *mut bool,
//    ),
//>;
//ExplainForeignScan_function =
//    ::std::option::Option<unsafe extern "C" fn(node: *mut ForeignScanState, es: *mut ExplainState)>;
//ExplainForeignModify_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        mtstate: *mut ModifyTableState,
//        rinfo: *mut ResultRelInfo,
//        fdw_private: *mut List,
//        subplan_index: ::std::os::raw::c_int,
//        es: *mut ExplainState,
//    ),
//>;
//ExplainDirectModify_function =
//    ::std::option::Option<unsafe extern "C" fn(node: *mut ForeignScanState, es: *mut ExplainState)>;
//AcquireSampleRowsFunc = ::std::option::Option<
//    unsafe extern "C" fn(
//        relation: Relation,
//        elevel: ::std::os::raw::c_int,
//        rows: *mut HeapTuple,
//        targrows: ::std::os::raw::c_int,
//        totalrows: *mut f64,
//        totaldeadrows: *mut f64,
//    ) -> ::std::os::raw::c_int,
//>;
//AnalyzeForeignTable_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        relation: Relation,
//        func: *mut AcquireSampleRowsFunc,
//        totalpages: *mut BlockNumber,
//    ) -> bool,
//>;
//ImportForeignSchema_function = ::std::option::Option<
//    unsafe extern "C" fn(stmt: *mut ImportForeignSchemaStmt, serverOid: Oid) -> *mut List,
//>;
//EstimateDSMForeignScan_function = ::std::option::Option<
//    unsafe extern "C" fn(node: *mut ForeignScanState, pcxt: *mut ParallelContext) -> Size,
//>;
//InitializeDSMForeignScan_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        node: *mut ForeignScanState,
//        pcxt: *mut ParallelContext,
//        coordinate: *mut ::std::os::raw::c_void,
//    ),
//>;
//ReInitializeDSMForeignScan_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        node: *mut ForeignScanState,
//        pcxt: *mut ParallelContext,
//        coordinate: *mut ::std::os::raw::c_void,
//    ),
//>;
//InitializeWorkerForeignScan_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        node: *mut ForeignScanState,
//        toc: *mut shm_toc,
//        coordinate: *mut ::std::os::raw::c_void,
//    ),
//>;
//ShutdownForeignScan_function =
//    ::std::option::Option<unsafe extern "C" fn(node: *mut ForeignScanState)>;
//IsForeignScanParallelSafe_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        root: *mut PlannerInfo,
//        rel: *mut RelOptInfo,
//        rte: *mut RangeTblEntry,
//    ) -> bool,
//>;
//ReparameterizeForeignPathByChild_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        root: *mut PlannerInfo,
//        fdw_private: *mut List,
//        child_rel: *mut RelOptInfo,
//    ) -> *mut List,
//>;
//
