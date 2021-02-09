use pg_sys::*;
use pgx::*;

// https://www.postgresql.org/docs/13/fdw-callbacks.html
pub type Tuple = (String, Option<pg_sys::Datum>, pgx::PgOid);

pub mod fdw_options {
    use pgx::*;
    use std::collections::HashMap;
    use std::ffi::CStr;

    pub type OptionMap = HashMap<String, String>;
    #[derive(Debug)]
    pub struct Options {
        pub server_opts: OptionMap,
        pub table_opts: OptionMap,
        pub table_name: String,
        pub table_namespace: String,
    }

    pub unsafe fn from_relation(relation: &PgRelation) -> Options {
        let table = PgBox::<pg_sys::ForeignTable>::from_pg(pg_sys::GetForeignTable(relation.rd_id));
        let server =
            PgBox::<pg_sys::ForeignServer>::from_pg(pg_sys::GetForeignServer(table.serverid));

        Options {
            server_opts: from_pg_list(server.options),
            table_opts: from_pg_list(table.options),
            table_name: relation.name().into(),
            table_namespace: relation.namespace().into(),
        }
    }

    fn from_pg_list(opts: *mut pg_sys::List) -> OptionMap {
        if opts.is_null() {
            return HashMap::new();
        }

        let pg_list = PgList::<pg_sys::DefElem>::from_pg(opts);

        pg_list
            .iter_ptr()
            .map(|ptr| unsafe { elem_to_tuple(ptr) })
            .collect::<OptionMap>()
    }

    unsafe fn elem_to_tuple(elem: *mut pg_sys::DefElem) -> (String, String) {
        let key = (*elem).defname;
        let value = (*((*elem).arg as *mut pg_sys::Value)).val.str_;

        match (CStr::from_ptr(key).to_str(), CStr::from_ptr(value).to_str()) {
            (Ok(k), Ok(v)) => (k.into(), v.into()),
            (Err(err), _) => error!("Unicode error {}", err),
            (_, Err(err)) => error!("Unicode error {}", err),
        }
    }
}

pub trait ForeignData {
    type Item: IntoDatum;
    type RowIterator: Iterator<Item = Vec<Self::Item>>;

    fn begin(options: &fdw_options::Options) -> Self;
    fn execute(&mut self, desc: &PgTupleDesc) -> Self::RowIterator;
    fn indices(_options: &fdw_options::Options) -> Option<Vec<String>> {
        None
    }

    fn insert(&mut self, _desc: &PgTupleDesc, _row: Vec<Tuple>) -> Option<Vec<Tuple>> {
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
    unsafe extern "C" fn GetForeignRelSize(
        _root: *mut PlannerInfo,
        baserel: *mut RelOptInfo,
        _foreigntableid: Oid,
    ) {
        (*baserel).rows = 0.0;
    }

    unsafe extern "C" fn GetForeignPaths(
        root: *mut PlannerInfo,
        baserel: *mut RelOptInfo,
        foreigntableid: Oid,
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

    unsafe extern "C" fn GetForeignPlan(
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

    unsafe extern "C" fn beginforeignscan(
        node: *mut ForeignScanState,
        eflags: ::std::os::raw::c_int,
    ) {
        let rel = PgRelation::from_pg((*node).ss.ss_currentRelation);
        let mut fdw_state = PgBox::<Self>::alloc0();
        let opts = fdw_options::from_relation(&rel);

        fdw_state.state = T::begin(&opts);
        fdw_state.itr = std::ptr::null_mut();

        (*node).fdw_state = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;
    }

    unsafe extern "C" fn IterateForeignScan(node: *mut ForeignScanState) -> *mut TupleTableSlot {
        let mut fdw_state = PgBox::<Self>::from_pg((*node).fdw_state as *mut Self);
        let mut fdw_itr = PgBox::<T::RowIterator>::from_pg(fdw_state.itr);

        let tupdesc = PgTupleDesc::from_pg_copy((*(*node).ss.ss_currentRelation).rd_att);
        let slot = Self::exec_clear_tuple((*node).ss.ss_ScanTupleSlot);
        let (item, itr_ptr) = Self::itr_next(&mut fdw_itr, &mut fdw_state, &tupdesc);

        let ret = match item {
            Some(row) => Self::store_tuple(slot, &tupdesc, row),
            None => slot,
        };

        fdw_state.itr = itr_ptr;
        (*node).fdw_state = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;

        ret
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

    unsafe fn store_tuple(
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

        let tuple =
            pg_sys::heap_form_tuple(tupdesc.as_ptr(), datums.as_mut_ptr(), nulls.as_mut_ptr());

        pg_sys::ExecStoreHeapTuple(tuple, slot, false)
    }

    unsafe fn exec_clear_tuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
        match (*(*slot).tts_ops).clear {
            Some(clear_fun) => {
                clear_fun(slot);
                return slot;
            }
            None => error!(""),
        };
    }

    unsafe fn get_some_attrs(slot: *mut TupleTableSlot, natts: i32) -> *mut TupleTableSlot {
        match (*(*slot).tts_ops).getsomeattrs {
            Some(fun) => fun(slot, natts),
            None => error!(""),
        }
        slot
    }

    unsafe extern "C" fn ReScanForeignScan(node: *mut ForeignScanState) {}

    unsafe extern "C" fn EndForeignScan(node: *mut ForeignScanState) {}

    unsafe extern "C" fn AddForeignUpdateTargets(
        parsetree: *mut Query,
        target_rte: *mut RangeTblEntry,
        target_relation: Relation,
    ) {
        let rel = PgRelation::from_pg(target_relation);
        let opts = fdw_options::from_relation(&rel);
        let tupdesc = PgTupleDesc::from_pg_copy((*target_relation).rd_att);

        if let Some(keys) = T::indices(&opts) {
            // Build a map of column names to attributes and column index
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

                    // TODO: error handling

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

    unsafe extern "C" fn BeginForeignModify(
        mtstate: *mut ModifyTableState,
        rinfo: *mut ResultRelInfo,
        fdw_private: *mut List,
        subplan_index: ::std::os::raw::c_int,
        eflags: ::std::os::raw::c_int,
    ) {
        let rel = PgRelation::from_pg((*rinfo).ri_RelationDesc);
        let opts = fdw_options::from_relation(&rel);
        let tupdesc = PgTupleDesc::from_pg_copy(rel.rd_att);
        let wrapper = Box::new(Self {
            state: T::begin(&opts),
            itr: std::ptr::null_mut(),
        });

        (*rinfo).ri_FdwState = Box::into_raw(wrapper) as pgx::memcxt::void_mut_ptr;
    }

    unsafe extern "C" fn ExecForeignInsert(
        estate: *mut EState,
        rinfo: *mut ResultRelInfo,
        slot: *mut TupleTableSlot,
        planSlot: *mut TupleTableSlot,
    ) -> *mut TupleTableSlot {
        let mut fdw_state = PgBox::<Self>::from_pg((*rinfo).ri_FdwState as *mut Self);
        let tupdesc = PgTupleDesc::from_pg_copy((*slot).tts_tupleDescriptor);
        let tuples = Self::slot_to_tuples(slot, &tupdesc);
        let _result = fdw_state.state.insert(&tupdesc, tuples);

        (*rinfo).ri_FdwState = fdw_state.into_pg() as pgx::memcxt::void_mut_ptr;
        slot
    }

    unsafe fn slot_to_tuples(slot: *mut TupleTableSlot, tupdesc: &PgTupleDesc) -> Vec<Tuple> {
        let slot = if (*slot).tts_nvalid == 0 {
            Self::get_some_attrs(slot, tupdesc.natts)
        } else {
            slot
        };

        let datums: &[pg_sys::Datum] =
            std::slice::from_raw_parts((*slot).tts_values, (*slot).tts_nvalid as usize);
        let nulls = std::slice::from_raw_parts((*slot).tts_isnull, (*slot).tts_nvalid as usize);

        let tuples: Vec<Tuple> = tupdesc
            .iter()
            .enumerate()
            .map(|(i, attr)| {
                let oid = attr.type_oid();
                (
                    attr.name().into(),
                    pg_sys::Datum::from_datum(
                        datums[i].to_owned(),
                        nulls[i].to_owned(),
                        oid.value(),
                    ),
                    oid,
                )
            })
            .collect();

        tuples
    }

    unsafe extern "C" fn ExecForeignDelete(
        estate: *mut EState,
        rinfo: *mut ResultRelInfo,
        slot: *mut TupleTableSlot,
        plan_slot: *mut TupleTableSlot,
    ) -> *mut TupleTableSlot {
        let mut fdw_state = PgBox::<Self>::from_pg((*rinfo).ri_FdwState as *mut Self);
        let tupdesc = PgTupleDesc::from_pg_copy((*plan_slot).tts_tupleDescriptor);
        let tuples = Self::slot_to_tuples(plan_slot, &tupdesc);
        let _result = fdw_state.state.delete(&tupdesc, tuples);

        slot
    }

    pub fn into_datum() -> pg_sys::Datum {
        let mut handler = PgBox::<pg_sys::FdwRoutine>::alloc_node(pg_sys::NodeTag_T_FdwRoutine);

        handler.GetForeignRelSize = Some(Self::GetForeignRelSize);
        handler.GetForeignPaths = Some(Self::GetForeignPaths);
        handler.GetForeignPlan = Some(Self::GetForeignPlan);
        handler.BeginForeignScan = Some(Self::beginforeignscan);
        handler.IterateForeignScan = Some(Self::IterateForeignScan);
        handler.ReScanForeignScan = Some(Self::ReScanForeignScan);
        handler.EndForeignScan = Some(Self::EndForeignScan);
        handler.EndForeignInsert = None;
        handler.ReparameterizeForeignPathByChild = None;
        handler.ShutdownForeignScan = None;
        handler.ReInitializeDSMForeignScan = None;
        handler.GetForeignJoinPaths = None;
        handler.GetForeignUpperPaths = None;
        handler.AddForeignUpdateTargets = Some(Self::AddForeignUpdateTargets);
        handler.PlanForeignModify = None;
        handler.BeginForeignModify = Some(Self::BeginForeignModify);
        handler.ExecForeignInsert = Some(Self::ExecForeignInsert);
        handler.ExecForeignUpdate = None;
        handler.ExecForeignDelete = Some(Self::ExecForeignDelete);
        handler.EndForeignModify = None;
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
//ExecForeignDelete_function = ::std::option::Option<
//    unsafe extern "C" fn(
//        estate: *mut EState,
//        rinfo: *mut ResultRelInfo,
//        slot: *mut TupleTableSlot,
//        planSlot: *mut TupleTableSlot,
//    ) -> *mut TupleTableSlot,
//>;
//EndForeignModify_function =
//    ::std::option::Option<unsafe extern "C" fn(estate: *mut EState, rinfo: *mut ResultRelInfo)>;
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
