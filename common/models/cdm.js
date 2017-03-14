//var loopback = require('loopback');
var _ = require('lodash');

module.exports = function(cdm) {
  concepts(cdm);
  //sampleUsers(cdm);
  cacheDirty(cdm);
  //drugConcepts(cdm);
  //exposureQueries(cdm); // code is still here but not being used (yet) in this project
}
function concepts(cdm) { // consolidating?
  var returns = { arg: 'data', type: ['cdm'], root: true };
  const schemaArgs = [
    {arg: 'cdmSchema', type: 'string', required: true },
    {arg: 'resultsSchema', type: 'string', required: true},
    //{arg: 'rowLimit', type: 'integer', required: false},
    {arg: 'req', type: 'object', http: { source: 'req' }},
  ];
  const filterArgs = [
    {arg: 'excludeInvalidConcepts', type: 'boolean', required: false, default: true},
    {arg: 'excludeNoMatchingConcepts', type: 'boolean', required: false, default: true},
    {arg: 'excludeNonStandardConcepts', type: 'boolean', required: false, default: false},
    {arg: 'includeFiltersOnly', type: 'boolean', required: false, default: false},
    {arg: 'includeInvalidConcepts', type: 'boolean', required: false, default: true},
    {arg: 'includeNoMatchingConcepts', type: 'boolean', required: false, default: true},
    {arg: 'includeNonStandardConcepts', type: 'boolean', required: false, default: false},
    {arg: 'domain_id', type: 'string', required: false,
              validCheck: v => typeof v === 'undefined' || 
                                _.includes([
                                  //'Drug','Condition','Procedure'
                                  "Condition/Device", "Gender", "Race", "Ethnicity", "Metadata", "Visit", "Procedure", "Modifier", "Drug", "Route", "Unit", "Device", "Condition", "Measurement", "Meas Value Operator", "Meas Value", "Observation", "Relationship", "Place of Service", "Provider Specialty", "Currency", "Revenue Code", "Specimen", "Spec Anatomic Site", "Spec Disease Status", "Device/Procedure", "Obs/Procedure", "Measurement/Obs", "Device/Obs", "Condition/Meas", "Condition/Obs", "Condition/Procedure", "Device/Drug", "Drug/Measurement", "Drug/Obs", "Condition/Drug", "Drug/Procedure", "Meas/Procedure", "Type Concept"
                                ],v)},
    {arg: 'grpset', type: 'string', required: false,
              validCheck: v => typeof v === 'undefined' || 
                                _.includes([
                                  'domain_id,standard_concept,vocabulary_id'
                                ],v)},
  ];
  /*
  const groupByArgs = [
    {arg: 'groupBy', type: ['string'], required: false,
              validCheck: v => _.isEmpty(v) ||
                               Array.isArray(v) &&
                               _.every(v, v => _.includes([
                                        'domain_1','vocab_1','class_1',
                                        'sc_1','table_1','column_1','type_1',
                                        'domain_2','vocab_2','class_2',
                                        'sc_2','table_2','column_2','type_2',
                                        ],v)) },
  ];
  */
  const otherArgs = [
    {arg: 'dataRequested', type: 'string', required: false},
    {arg: 'queryName', type: 'string', required: false, default: 'All concept stats'},
    {arg: 'targetOrSource', type: 'string', required: false, default: 'target',
              validCheck: v => _.includes(['target','source','both'],v)},
    {arg: 'includeTypeCol', type: 'boolean', required: false, default: false},
    //{arg: 'query', type: 'string', required: true},
  ];
  var accepts = [].concat(schemaArgs, filterArgs, otherArgs);
  let classAccepts = accepts.slice(0).concat(
    //groupByArgs,
    //{arg: 'domain_id', type: 'string', required: false },
    {arg: 'hierarchical', type: 'string', required: false, default: 'either',
              validCheck: v => _.includes(['is_hierarchical','defines_ancestry','both', 'either','neither'],v)}
  ) 

  cdm.conceptCounts = function(..._params) {
    var accepts = [].concat(schemaArgs, filterArgs, otherArgs);
    const cb = _params.pop();
    let [params,req] = toNamedParams(_params, accepts);
    //console.log(params);
    let sql;
    sql = conceptSql(params);
    /* got rid of separate target/source concepts in cio table
    if (params.targetOrSource === 'both') {
      sql = conceptSql(_.merge({},params,{targetOrSource:'target'}),true)
            + '\nunion\n' +
            conceptSql(_.merge({},params,{targetOrSource:'source'}),true)
    } else {
      sql = conceptSql(params);
    }
    */
    runQuery(cdm, cb, req, sql, params);
  };

  cdm.remoteMethod('conceptCounts', { accepts, returns, accessType: 'READ', http: { verb: 'post' } });
  cdm.remoteMethod('conceptCounts', { accepts, returns, accessType: 'READ', http: { verb: 'get' } });

  function conceptSql(params, targetSourceCol = false) {
    let filters = filterConditions(params);
    let cols = [];
    let countCols = [ `coalesce(sum(cio.cnt),0) AS record_count`,
                      `count(*) AS concept_count` ];
    let conceptCols = [];
    let additionalBreakdownCols = [ 
                                    'c.invalid_reason', 
                                    'c.standard_concept', 
                                    'c.domain_id', 
                                    'c.vocabulary_id', 
                                    'c.concept_class_id'];

    let typeJoin = '';
    if (params.includeTypeCol) {
      console.warn("no longer using type_concept_id in cio table");
      //typeJoin = `JOIN ${params.cdmSchema}.concept ct on cio.type_concept_id = ct.concept_id and ct.invalid_reason is null`;
      //additionalBreakdownCols.unshift('ct.concept_name as type_concept_name');
    }

    // validated targetOrSource already
    // quit using targetOrSource
    // let conceptCol = `cio.${params.targetOrSource}_concept_id`;
    // let conceptColName = `cio.${params.targetOrSource}_column_name as column_name`;
    let conceptCol = `cio.concept_id`;
    let conceptColName = `cio.column_name as column_name`;

    additionalBreakdownCols.unshift(conceptColName);

    if (targetSourceCol)
      additionalBreakdownCols.unshift(`'${params.targetOrSource}' as targetOrSource`);

    additionalBreakdownCols.unshift('cio.table_name');

    let groupBy = '';
    switch (params.dataRequested) {
      case 'counts':
        cols = cols.concat(countCols);
        break;
      case 'agg':
        cols = cols.concat(conceptCols, additionalBreakdownCols, countCols);
        groupBy = `group by ${_.range(1, cols.length - countCols.length + 1)}`;
        break;
      case 'details':
        cols = cols.concat(conceptCols, additionalBreakdownCols, countCols);
        groupBy = `group by ${_.range(1, cols.length - countCols.length + 1)}`;
        break;
      /*
      case 'target':
        cols = `
                  c.concept_name AS concept_name,
                  ct.concept_name AS type_concept_name,
                  c.invalid_reason, 
                  c.standard_concept, 
                  c.domain_id, 
                  c.vocabulary_id, 
                  c.concept_class_id,
                  cio.concept_id AS concept_id,
                  cio.type_concept_id AS type_concept_id,
                ` + cols;
        break;
      case 'source':
        throw new Error("not handling yet");
      */
      default:
        throw new Error(`not handling conceptCounts/${params.dataRequested} yet`);
    }

    let sql = `
          SELECT  ${cols.join(', ')}
          FROM ${params.resultsSchema}.concept_id_occurrence cio
          JOIN ${params.cdmSchema}.concept c ON ${conceptCol} = c.concept_id
          ${typeJoin}
          ${where(filters)}
          ${groupBy}
        `;
    return sql;
    /*
    switch (query) {
      case 'conceptStats':
        if (attr) {
          if (excludeInvalidConcepts) filters.push('invalid_reason is null');
          if (excludeNoMatchingConcepts) filters.push('concept_id != 0');
          if (excludeNonStandardConcepts) filters.push('standard_concept is not null');
          sql = `
                  select ${attr}, sum(count) as dbrecs, count(*) as conceptrecs
                  from ${resultsSchema}.concept_info
                  ${where(filters)}
                  group by 1`;
        } else {
          if (excludeInvalidConcepts) filters.push('invalid = false');
          if (excludeNoMatchingConcepts) filters.push(`vocabulary_id != 'None'`);
          if (excludeNonStandardConcepts) filters.push('standard_concept is not null');
          sql = `
                  select *
                  from ${resultsSchema}.concept_info_stats
                  ${where(filters)}
                `;
        }
        break;
      case 'conceptCount':
        if (excludeInvalidConcepts) filters.push('invalid_reason is null');
        if (excludeNoMatchingConcepts) filters.push('concept_id != 0');
        if (excludeNonStandardConcepts) filters.push('standard_concept is not null');
        sql = `
                select count(*) as count
                from ${cdmSchema}.concept
                ${where(filters)}
              `;
        break;
      case 'classRelations':
        if (excludeInvalidConcepts) filters.push(`invalid_1 = false`, `invalid_2 = false`);
        if (excludeNoMatchingConcepts) filters.push(`vocab_1 != 'None'`, `vocab_2 != 'None'`);
        if (excludeNonStandardConcepts) filters.push('sc_1 is not null', 'sc_2 is not null');
        sql =  `
                select * 
                from ${resultsSchema}.class_relations 
                ${where(filters)}
                order by 1,2,5,6,11,8,9,10,16,13,14,15`;
    }
    */
  }

  function filterConditions(params) {
    let filters = [];
    if (params.domain_id) {
      filters.push(`c.domain_id = '${params.domain_id}'`);
    }
    if (params.includeFiltersOnly) {
      let ors = [];
      if (params.includeInvalidConcepts) ors.push('c.invalid_reason is not null');
      if (params.includeNoMatchingConcepts) ors.push('c.concept_id = 0');
      if (params.includeNonStandardConcepts) ors.push('c.standard_concept is null');
      ors.length && filters.push(orItems(ors));
    } else {
      let ands = [];
      if (params.excludeInvalidConcepts) ands.push('c.invalid_reason is null');
      if (params.excludeNoMatchingConcepts) ands.push('c.concept_id != 0');
      if (params.excludeNonStandardConcepts) ands.push('c.standard_concept is not null');
      ands.length && filters.push(andItems(ands));
    }
    return filters;
  }
  cdm.concept_groups = function(..._params) {
    const cb = _params.pop();
    let [params,req] = toNamedParams(_params, schemaArgs);
    let sql = `
        select cg.*
        from ${params.resultsSchema}.concept_groups cg 
        `;
    let rowTransform = d => {
        d.cc = parseInt(d.cc,10);
        d.rc_rowcnt = parseInt(d.rc_rowcnt,10);
        d.tblcols = parseInt(d.tblcols,10);
        d.rc = parseInt(d.rc,10);
        d.src = parseInt(d.src,10);
        d.cidcnt = parseInt(d.cidcnt,10);
        d.dcc = parseInt(d.dcc,10);
        d.drc = parseInt(d.drc,10);
        d.dsrc = parseInt(d.dsrc,10);
        d.grpset.forEach((fld,i) => d[fld] = d.vals[i]);
        return d;
    };
    runQuery(cdm, cb, req, sql, params, rowTransform);
  }
  cdm.remoteMethod('concept_groups', { accepts:schemaArgs, returns, accessType: 'READ', http: { verb: 'post' } });
  cdm.remoteMethod('concept_groups', { accepts:schemaArgs, returns, accessType: 'READ', http: { verb: 'get' } });

  generateRemoteMethod({
    apiName:'dcid_cnts_breakdown', cdm, returns,
    accepts:schemaArgs,
    sqlTemplate: params => `
        select dcid_grp_id, cgids, grp, grpset, vals, dcc, drc_rowcnt, dtblcols,
               drc, dsrc
        from ${params.resultsSchema}.dcid_cnts_breakdown dg 
        `,
    rowTransform: d => {
        d.dcc = parseInt(d.dcc,10);
        d.drc_rowcnt = parseInt(d.drc_rowcnt,10);
        d.dtblcols = parseInt(d.dtblcols,10);
        d.drc = parseInt(d.drc,10);
        d.dsrc = parseInt(d.dsrc,10);
        return d;
    }});
  generateRemoteMethod({
    apiName:'conceptAncestors', cdm, returns,
    accepts:schemaArgs.concat({arg: 'concept_id', type: 'number', required: true}),
    sqlTemplate: params => `
        select *
        from ${params.resultsSchema}.ancestors
        where descendant_concept_id = ${params.concept_id}
        `});
  generateRemoteMethod({
    apiName:'conceptAncestorGroups', cdm, returns,
    accepts:schemaArgs.concat({arg: 'concept_id', type: 'number', required: true}),
    sqlTemplate: params => `
        select  case when ancestor_concept_id = ${params.concept_id} then 'ancestor rec' else 'descendant rec' end as role,
                a_domain_id, a_standard_concept, a_vocabulary_id, a_concept_class_id,
                d_domain_id, d_standard_concept, d_vocabulary_id, d_concept_class_id,
                count(*) cc, 
                array_unique(array_agg(min_levels_of_separation)) min_levels
        from ${params.resultsSchema}.ancestors
        where descendant_concept_id = ${params.concept_id}
        group by 1,2,3,4,5,6,7,8,9
        order by 1,2,3,4,5,6,7,8,9
        `,
    rowTransform: d => {
        d.cc = parseInt(d.cc,10);
        return d;
    }});
  generateRemoteMethod({
    apiName:'conceptDescendants', cdm, returns,
    accepts:schemaArgs.concat({arg: 'concept_id', type: 'number', required: true}),
    sqlTemplate: params => `
        select *
        from ${params.resultsSchema}.ancestors
        where ancestor_concept_id = ${params.concept_id}
        `});
  generateRemoteMethod({
    apiName:'conceptDescendantGroups', cdm, returns,
    accepts:schemaArgs.concat({arg: 'concept_id', type: 'number', required: true}),
    sqlTemplate: params => `
        select  case when ancestor_concept_id = ${params.concept_id} then 'ancestor rec' else 'descendant rec' end as role,
                a_domain_id, a_standard_concept, a_vocabulary_id, a_concept_class_id,
                d_domain_id, d_standard_concept, d_vocabulary_id, d_concept_class_id,
                count(*) cc, 
                array_unique(array_agg(min_levels_of_separation)) min_levels
        from ${params.resultsSchema}.ancestors
        where ancestor_concept_id = ${params.concept_id}
        group by 1,2,3,4,5,6,7,8,9
        order by 1,2,3,4,5,6,7,8,9
        `,
    rowTransform: d => {
        d.cc = parseInt(d.cc,10);
        return d;
    }});
  generateRemoteMethod({
    apiName:'conceptRecord', cdm, returns,
    accepts:schemaArgs.concat( {arg: 'concept_id', type: 'number', required: true}),
    sqlTemplate: params => `
        select  concept_id,
                concept_code,
                concept_name,
                domain_id,
                standard_concept,
                vocabulary_id,
                concept_class_id,
                array_agg((
                  select row_to_json(_) 
                  from (select rc.tbl, rc.col, rc.coltype, rc.rc, rc.src) as _
                )) as rcs
        from ${params.resultsSchema}.record_counts rc
        where concept_id = $1
        group by 1,2,3,4,5,6,7
        `,
        //where ${ typeof params.concept_id !== 'undefined' && 'concept_id = $1' || params.concept_code && 'concept_code = $1' || 'null = $1' }
    qpFunc: p => [p.concept_id],
    resultsTransform: d => d && d[0] || null,
  });
  generateRemoteMethod({
    apiName:'conceptRecordsFromCode', cdm, returns,
    accepts:schemaArgs.concat({arg: 'concept_code', type: 'string', required: true}),
    sqlTemplate: params => `
        select  concept_id,
                concept_code,
                concept_name,
                domain_id,
                standard_concept,
                vocabulary_id,
                concept_class_id,
                array_agg((
                  select row_to_json(_) 
                  from (select rc.tbl, rc.col, rc.coltype, rc.rc, rc.src) as _
                )) as rcs
        from ${params.resultsSchema}.record_counts rc
        where concept_code = $1
        group by 1,2,3,4,5,6,7
        `,
    qpFunc: p => [p.concept_code],
    //rowTransform: d => { d.rc = parseInt(d.rc,10); d.src = parseInt(d.src,10); return d; }
  });
  generateRemoteMethod({
    apiName:'relatedConcepts', cdm, returns,
    accepts:schemaArgs.concat(
      {arg: 'concept_id', type: 'number', required: true},
      {arg: 'maps', type: 'boolean', required: false}
    ),
    sqlTemplate: p => `
        select  r.defines_ancestry,
                r.is_hierarchical,
                r.relationship_id,
                r.relationship_name,
                r.reverse_relationship_id,

                c.domain_id,
                c.standard_concept,
                c.vocabulary_id,
                c.concept_class_id,

                c.concept_id,
                c.concept_code,
                c.concept_name

                from ${p.cdmSchema}.concept_relationship cr
                join ${p.cdmSchema}.relationship r 
                      on cr.relationship_id=r.relationship_id
                join ${p.cdmSchema}.concept c 
                      on cr.concept_id_2 = c.concept_id

                where cr.concept_id_1 = $1
                  and cr.invalid_reason is null
                  and cr.concept_id_2 != cr.concept_id_1
                  ${p.maps === true && "and cr.relationship_id in ('Maps to','Mapped from')"||''}
                  ${p.maps === false && "and cr.relationship_id not in ('Maps to','Mapped from')"||''}

                order by relationship_name, domain_id, standard_concept, 
                          vocabulary_id, concept_class_id, concept_name
          `,
    qpFunc: p=>[p.concept_id],
    rowTransform: d => {
        d.defines_ancestry = d.defines_ancestry === '1' ? true : false;
        d.is_hierarchical = d.is_hierarchical === '1' ? true : false;
        //d.rc = parseInt(d.rc,10);
        //d.src = parseInt(d.src,10);
        //d.crc = parseInt(d.crc,10);
        return d;
    }});
  generateRemoteMethod({
    apiName:'relatedConceptGroups', cdm, returns,
    accepts:schemaArgs.concat({arg: 'concept_id', type: 'number', required: true}),
    sqlTemplate: params => `
        select  r.defines_ancestry,
                r.is_hierarchical,
                r.relationship_id,
                r.relationship_name,
                r.reverse_relationship_id,

                c.domain_id,
                c.standard_concept,
                c.vocabulary_id,
                c.concept_class_id,

                count(distinct c.concept_id)::integer cc

                from ${params.cdmSchema}.concept_relationship cr
                join ${params.cdmSchema}.relationship r 
                      on cr.relationship_id=r.relationship_id
                join ${params.cdmSchema}.concept c 
                      on cr.concept_id_2 = c.concept_id

                where cr.concept_id_1 = ${params.concept_id}
                  and cr.invalid_reason is null
                  and cr.concept_id_2 != cr.concept_id_1

                group by 1,2,3,4,5,6,7,8,9
                order by relationship_name, domain_id, standard_concept,
                          vocabulary_id, concept_class_id
          `,
    rowTransform: d => {
        d.defines_ancestry = d.defines_ancestry === '1' ? true : false;
        d.is_hierarchical = d.is_hierarchical === '1' ? true : false;
        //d.cc = parseInt(d.cc,10);
        //d.rc = parseInt(d.rc,10);
        //d.src = parseInt(d.src,10);
        //d.crc = parseInt(d.crc,10);
        return d;
    }});

  let conceptInfoAccepts = [].concat(schemaArgs, 
          {arg: 'concept_id', type: 'number', required: true},
          {arg: 'maps', type: 'boolean', required: false} // what related stuff to fetch?
  );
  cdm.conceptInfo = function(..._params) {
    const cb = _params.pop();
    //console.log('conceptInfo params', _params);
    //console.log('conceptInfoAccepts', conceptInfoAccepts);
    let [params,req] = toNamedParams(_params, conceptInfoAccepts);
    let source = 'calledFromMethod';
    let recPromise = cdm.conceptRecord(source, params, cb, req);
    recPromise.then(
      rec => {
        //console.log("got something back from concept record: ", rec);
        if (_.isEmpty(rec)) {
          console.log("no record for", _.pickBy(params, (v,k)=>k!=='req'));
          cb(null, null);
          //cb("no record for " + params.concept_id, rec);
          return;
        }
        params.concept_id = rec.concept_id;
        //console.log("about to run relatedConcepts with", _.keys(params));
        let relatedConcepts = cdm.relatedConcepts(source,params,cb,req).then(d=>rowLimit(d,100));
        let relatedConceptGroups = cdm.relatedConceptGroups(source,params,cb,req);
        let relatedConceptCount = relatedConceptGroups.then(
          ({rows,apiName}={}) => {return {rows:_.sum(rows.map(d=>d.cc)), apiName:'relatedConceptCount'}});

        //_params[_.findIndex(conceptInfoAccepts,d=>d.arg==='concept_id')] = rec.concept_id;
        let conceptAncestors = cdm.conceptAncestors(source,params,cb,req)
                                    .then(d=>rowLimit(d,100));
        let conceptAncestorGroups = cdm.conceptAncestorGroups(source,params,cb,req);
        let conceptAncestorCount = conceptAncestorGroups.then(
          ({rows,apiName}={}) => {
            return {rows:_.sum(rows.map(d=>d.cc)), apiName:'conceptAncestorCount'}
          });

        let conceptDescendants = cdm.conceptDescendants(source,params,cb,req)
                                    .then(d=>rowLimit(d,100));
        let conceptDescendantGroups = cdm.conceptDescendantGroups(source,params,cb,req)
        let conceptDescendantCount = conceptDescendantGroups.then(
          ({rows,apiName}={}) => {
            return {rows:_.sum(rows.map(d=>d.cc)), apiName:'conceptDescendantCount'}
          });

        let promises = [
                        {apiName:'conceptRecord', rows:rec},
                        conceptAncestors, conceptAncestorGroups,
                        conceptDescendants, conceptDescendantGroups,
                        conceptAncestorCount, conceptDescendantCount,

                        relatedConcepts, relatedConceptGroups, relatedConceptCount,
                      ];
        //console.log('conceptInfo', cb, promises);
        multipleResultSets(cb, promises)
      },
      ({err, rows, } = {}) => {
        console.log("got error back from concept record", err, rows);
        cb(err||null, rows||null);
        //throw new Error("got error back from concept record", err, rows);
      });
  }
  cdm.remoteMethod('conceptInfo', { accepts:conceptInfoAccepts, returns, accessType: 'READ', http: { verb: 'post' } });
  cdm.remoteMethod('conceptInfo', { accepts:conceptInfoAccepts, returns, accessType: 'READ', http: { verb: 'get' } });

  generateRemoteMethod({
    apiName:'cdmRecs', cdm, returns,
    accepts:schemaArgs.concat(
      {arg: 'concept_id', type: 'number', required: true},
      {arg: 'tbl', type: 'string', required: true},
      {arg: 'col', type: 'string', required: true},
      {arg: 'rowLimit', type: 'number', required: false}
    ),
    rowTransform: d=>d.rec,
    sqlTemplate: params => `
        select dsql as rec
        from ${params.resultsSchema}.dsql('${params.cdmSchema}.${params.tbl}'::regtype,
                                          quote_ident('${params.col}'),
                                          ${params.concept_id}
                                          ${_.isNumber(params.rowLimit) ? ','+params.rowLimit : ''}
                                        );
        `});


  /*
  cdm.conceptGroups = function(..._params) {
    throw new Error("not sure if conceptGroups still being used");
    //var accepts = [].concat(schemaArgs, filterArgs, otherArgs);
    const cb = _params.pop();
    let [params,req] = toNamedParams(_params, classAccepts);
    let filters = [], vals = [];
    // FIX SQL INJECT PROBLEM!!! (use query params)
    filters = ['domain_id','standard_concept','vocabulary_id',
               'concept_class_id','tbl','col','coltype']
      .filter(fld => params[fld])
      .map(fld => `#TBL#.vals[array_position(#TBL#.grpset, '${fld}')] = '${params[fld]}'`);
    if (params.grpset) {
      let cols = params.grpset.split(/,/);
      cols = cols.map(d=>`'${d}'`).join(',');
      filters.push(`#TBL#.grpset = array[${cols}]`); // fix sql inject
      //vals.push(params.grpset);
    }
    let where = filters.length ?
                  `where ${filters.join(' and ').replace(/#TBL#/g,'cg')}` : '';
    /*
    let sql = `
                select *
                from ${params.resultsSchema}.concept_groups cg 
                ${where}
              `;
    * /
    let sql = `
        select cg.*,
              array_unique(array_agg(array_to_string(dg.vals,','))) linknodes
              --,sum(dg.dcc), sum(dg.dtblcols), sum(dg.drc), sum(dg.dsrc)
        from ${params.resultsSchema}.concept_groups cg 
        left join ${params.resultsSchema}.dcid_cnts_breakdown dg 
            on cg.dcid_grp_id = dg.dcid_grp_id and
               cg.grpset = dg.grpset and
               cg.vals[1] = dg.vals[1] and
               ${filters.join(' and ').replace(/#TBL#/g,'dg')}
        ${where}
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14`;
    let rowTransform = d => {
        d.cc = parseInt(d.cc,10);
        d.rc_rowcnt = parseInt(d.rc_rowcnt,10);
        d.tblcols = parseInt(d.tblcols,10);
        d.rc = parseInt(d.rc,10);
        d.src = parseInt(d.src,10);
        d.cidcnt = parseInt(d.cidcnt,10);
        d.dcc = parseInt(d.dcc,10);
        d.drc = parseInt(d.drc,10);
        d.dsrc = parseInt(d.dsrc,10);
        d.grpset.forEach((fld,i) => d[fld] = d.vals[i]);
        return d;
    };
    runQuery(cdm, cb, req, sql, params, rowTransform, true);
  };
  console.log('registering conceptGroups');
  cdm.remoteMethod('conceptGroups', { accepts:classAccepts, returns, accessType: 'READ', http: { verb: 'post' } });
  cdm.remoteMethod('conceptGroups', { accepts:classAccepts, returns, accessType: 'READ', http: { verb: 'get' } });
  */
}
function sampleUsers(cdm) {
  var returns = { arg: 'data', type: ['cdm'], root: true };
  cdm.sampleUsersGet = cdm.sampleUsersPost = 
    function(cdmSchema, resultsSchema, concept_id, sampleCnt, 
             measurename, bundle, entityName, maxgap, from, to, queryName, cb) {
      resultsSchema = cdmSchema + '_results';
      var ds = cdm.dataSource;
      let allParams = 
        {cdmSchema, resultsSchema, concept_id, queryName, sampleCnt, 
         maxgap, from, to, bundle, entityName, measurename, measureCol};
      var sql = '', filter = '', measureCol;


      // confusion around bundle and entityName right now...
      // different terms being used sort of same way...have to fix

      switch (entityName) {
        case 'era':
          bundle = 'era';
          break;
        case 'exposure':
          bundle = 'exp';
          break;
        default:
      }

      if (bundle === 'era') {
        measureCol =
          ({ 
            gap: 'btn_era_gap_days',
            exp_count: 'exp_count',
            exposures: 'exposures',
            duration: 'era_days',
            //duration: 'max(drug_exposure_end) - min(drug_exposure_start_date)',
          })[measurename];
        if (typeof from !== 'undefined' && typeof to !== 'undefined') {
          filter =  ` where ${measureCol} between @from and @to `;
        }
        sql = plainEras({resultsSchema, maxgap, concept_id, undefined, undefined, measurename, bundle, entityName, filter});
        sql = ` 
              /* sampleUsers */
              select person_id 
              from (${sql}
              ) plain_eras
              order by ${measureCol} desc 
              limit @sampleCnt `;
      } else if (bundle === 'exp') {
        measureCol =
          ({ 
            gap: 'days_from_latest',
            exp_count: 'exp_num',
            exposures: 'exp_num',
            overlap: 'days_from_latest',
            duration: 'days_supply',
          })[measurename];
        allParams.measureCol = measureCol;
        if (typeof from !== 'undefined' && typeof to !== 'undefined') {
          filter =  ` and ${measureCol} between @from and @to `;
        }
        let filt = '';
        if (measurename === 'overlap') {
          filt = ` and ${measureCol} < 0`
        }
        if (measurename === 'gap') {
          filt = ` and ${measureCol} > 0`
        }
        // can't come up with good way to get exactly sampleCnt
        // top persons
        sql = `
              /* sampleUsers exp */
              select person_id
              from ${resultsSchema}.drug_exposure_rollup
              where rollup_concept_id = @concept_id
              --${filt}
              order by ${measureCol} ${measurename==='overlap' ? 'ASC' : 'DESC'}
              limit 1000`;
        /*
          sql = `
                /* sampleUsers exp but not exp_num * /
                /*
                select person_id 
                from ${cdmSchema}_results.drug_by_person_stats
                where rollup_concept_id = @concept_id
                  ${filter}
                order by ${measureCol} desc 
                limit @sampleCnt 
                * /

                select distinct person_id
                from (
                  select distinct person_id, exp_num
                  from (${sql}
                  ) exposures
                order by ${measureCol} desc 
                ) p
                limit @sampleCnt `;
        */
      } else {
        console.error(`don't know what to do with bundle ${bundle}`, allParams);
      }
      let numParams = {concept_id, sampleCnt, maxgap, from, to};
      sql = resolveParams(sql, numParams);
      console.log('==============>\n', allParams, sql, '\n<==============\n');
      ds.connector.query(sql, [], function(err, rows) {
        if (err) console.error(err);
        cb(err, rows);
      });
    };

  var sampleUsersAccepts = [
      {arg: 'cdmSchema', type: 'string', required: true },
      {arg: 'resultsSchema', type: 'string', required: false},
      {arg: 'concept_id', type: 'number', required: false},
      {arg: 'sampleCnt', type: 'number', required: false, default: 2},
      {arg: 'measurename', type: 'string', required: true, },
      {arg: 'bundle', type: 'string', required: true, },
      {arg: 'entityName', type: 'string', required: false, },
      {arg: 'maxgap', type: 'number', required: false},
      {arg: 'from', type: 'number', required: false, },
      {arg: 'to', type: 'number', required: false, },
      {arg: 'queryName', type: 'string', required: false, default: 'No query name'},
  ];

  cdm.remoteMethod('sampleUsersGet', {
    accepts: sampleUsersAccepts,
    returns,
    accessType: 'READ',
    http: {
      verb: 'get'
    }
  });
  cdm.remoteMethod('sampleUsersPost', {
    accepts: sampleUsersAccepts,
    returns,
  });
}
function exposureQueries(cdm) {
  var returns = { arg: 'data', type: ['cdm'], root: true };
  cdm.sqlget = cdm.sqlpost = 
    function(aggregate, bundle, cdmSchema, resultsSchema, concept_id, person_id, 
             maxgap, ntiles, measurename, limit=2000, noLimit, queryName, cb) {
      var ds = cdm.dataSource;
      /*
       * can't figure out how to verify schema names or find schemas,
       *  so i'm just going to do something super unsafe
      console.log(ds.settings);
      var schemas = ds.discoverSchemasSync('cdm');
      console.log(schemas);
      var schemas2 = cdmDS.discoverSchemasSync('cdm');
      console.log(schemas2);
      var cdmDS = this.getDataSource(cdmSchema);
      var resultsDS = this.getDataSource(resultsSchema);
      console.log(resultsDS.settings);
      var cdmSchemaNameTrusted = cdmDS.settings.schema;
      var resultsSchemaNameTrusted = resultsDS.settings.schema;
      console.log(`cdm: ${cdmSchema}/${cdmSchemaNameTrusted}, results: ${resultsSchema}/${resultsSchemaNameTrusted}`);
      //console.log(resultsSchema, resultsDS.settings);
      */
      var sql; 
      var drugName = false; // whether to include it in rollup_exposure - only for non-aggregate exp
      if (!aggregate) {
        switch(bundle) {
          case 'exp':
            drugName = true;
          case 'allexp':
            sql = exposure_rollup({resultsSchema, concept_id, person_id, ntiles, measurename, bundle, drugName});
            sql += '\norder by person_id, rollup_concept_id, exp_num';
            if (sql.match(/ntiles/)) sql += ', ntiles';
            break;
          case 'era':
          case 'allera':
            sql = eras({resultsSchema, maxgap, concept_id, person_id, ntiles, measurename, bundle});
            sql += '\norder by person_id, rollup_concept_id, era_num';
            if (sql.match(/ntiles/)) sql += ', ntiles';
            break;
          case 'single':
            //sql = exposure_rollup({resultsSchema, concept_id, person_id, ntiles, measurename, limit});
            break;
        }
      } else {
        switch(bundle) {
          case 'exp':
          case 'allexp':
            sql = exposure_rollup({resultsSchema, concept_id, person_id, ntiles, measurename, bundle});
            sql = ntileCross(sql, {resultsSchema, concept_id, person_id, ntiles, measurename, bundle});
            break;
          case 'era':
          case 'allera':
            sql = eras({resultsSchema, maxgap, concept_id, person_id, ntiles, measurename, bundle});
            sql = ntileCross(sql, {resultsSchema, concept_id, person_id, ntiles, measurename, bundle});
            break;
          case 'single':
            //sql = exposure_rollup({resultsSchema, concept_id, person_id, ntiles, measurename, limit});
            break;
        }
      }
      let limitStr = '';
      if (!noLimit) {
        limitStr = `limit @limit`;
      }
      sql = `${sql} ${limitStr}`;
      let numParams = {concept_id, person_id, maxgap, ntiles, limit};
      sql = resolveParams(sql, numParams);
      let allParams = {aggregate, bundle, cdmSchema, resultsSchema, concept_id, person_id, 
             maxgap, ntiles, measurename, noLimit, queryName, drugName};
      console.log('==============\n', allParams, sql, '\n<==============\n');
      ds.connector.query(sql, [], function(err, rows) {
        if (err) console.error(err);
        cb(err, rows);
      });
    };

  var accepts = [
      {arg: 'aggregate', type: 'boolean', required: true, default: false},
      {arg: 'bundle', type: 'string', required: true,
                description: 'exp, era, or single'},
      //{arg: 'request', type: 'string', required: true},
      {arg: 'cdmSchema', type: 'string', required: false},
      {arg: 'resultsSchema', type: 'string', required: false},
      {arg: 'concept_id', type: 'number', required: false},
      {arg: 'person_id', type: 'number', required: false},
      {arg: 'maxgap', type: 'number', required: false},
      {arg: 'ntiles', type: 'number', required: false},
      {arg: 'measurename', type: 'string', required: false,
              description: 'should be duration, gap, or overlap'},
      {arg: 'limit', type: 'number', required: false},
      {arg: 'noLimit', type: 'boolean', required: false, default: false},
      {arg: 'queryName', type: 'string', required: false, default: 'No query name'},
  ];
  cdm.remoteMethod('sqlget', {
    accepts,
    returns,
    accessType: 'READ',
    http: {
      verb: 'get'
    }
  });
  cdm.remoteMethod('sqlpost', {
    accepts,
    returns,
  });
}
function resolveParams(sql, params) {
  // doing this this way so I can use real sql parameterization later
  //console.log(sql, params);
  return  sql.replace(/@(\w+)/g, function(match, token) {
            return params[token];
          });
}
function ntileCol(p, whereArr) {
  // may modify whereArr
  let {ntiles, measurename, bundle, entityName} = p;

  if (typeof ntiles === 'undefined' || typeof measurename === 'undefined')
    return '';

  let partition = ({
      exp: 'partition by exp_num', 
      era: 'partition by era_num', 
      allexp: '',
      allera: '',
  })[bundle];

  let order = ({
    duration: {
      exp: 'days_supply', 
      allexp: 'days_supply', 
      era: 'total_days_supply', 
      allera: 'total_days_supply', 
    },
    gap: {
      exp: 'days_from_latest', 
      allexp: 'days_from_latest', 
      era: 'btn_era_gap_days', 
      allera: 'btn_era_gap_days', 
    },
    overlap: {
      exp: 'exp_overlap_days', 
      allexp: 'exp_overlap_days', 
      era: 'NO_ERA_OVERLAP',
      allera: 'NO_ERA_OVERLAP',
    },
  })[measurename][bundle];

  if (measurename === 'gap' || measurename === 'overlap') {
    if (bundle === 'exp')
      whereArr.push(`exp_num > 1`);
    //if (bundle === 'allexp') whereArr.push(`exp_num > 1`); shouldn't matter
    if (bundle === 'era')
      whereArr.push(`era_num > 1`);
  }

  sql = `
                                    /* ntileCol */
                                    ntile(@ntiles) over (${partition} order by ${order}) ntile, `;
  return sql;
}
function exposure_rollup(p, parameterize) {
  let {resultsSchema, concept_id, person_id, ntiles, 
        measurename, bundle, entityName, drugName} = p;
  let where = [];
  if (typeof concept_id !== 'undefined') {
    where.push(`rollup_concept_id = @concept_id`);
  }
  if (typeof person_id !== 'undefined') {
    where.push(`person_id = @person_id`);
  }

  let ntilecol =
        (typeof ntiles === 'undefined' || typeof measurename === 'undefined')
        ? ''
        : ntileCol({ntiles, measurename, bundle, entityName}, where);

  let whereClause = '';
  if (where.length) {
    whereClause = `where ${where.join(' and ')}`;
  }
  let sql = `
                            select 
                                    drug_exposure_start_date -
                                      first_value(drug_exposure_start_date) 
                                        over (order by exp_num)
                                      as days_from_first,
                                    case when days_from_latest > 0 
                                        then days_from_latest else 0 end as exp_gap_days,
                                    case when days_from_latest < 0 
                                        then
                                            case when -days_from_latest > days_supply 
                                                  then days_supply
                                                  else -days_from_latest
                                            end
                                        else 0 
                                    end as exp_overlap_days,
                                    d.*
                                    ${drugName ? ',c.concept_name as drug_name' : ''}
                            from ${resultsSchema}.drug_exposure_rollup d
                            ${
                                drugName ?
                                  'join omop5_synpuf_5pcnt.concept c on d.drug_concept_id = c.concept_id'
                                  : ''
                            }
                            ${whereClause}
                            `;
  if (ntilecol) {
    sql = `
                            select ${ntilecol}
                                    er_no_ntile.*
                            from (${sql}
                            ) er_no_ntile `;
  }
  sql = `
                            /* exposure_rollup(${JSON.stringify(p)}) */
                            ${sql}`;

  return sql;
}
function ntileCross(sql, p, parameterize) {
  let {resultsSchema, concept_id, person_id,
            ntiles, measurename, bundle, entityName} = p;

  let aggs = {
    exp:    { duration: 'days_supply',        gap: 'days_from_latest', overlap: 'exp_overlap_days' },
    allexp: { duration: 'days_supply',        gap: 'days_from_latest', overlap: 'exp_overlap_days' },
    era:    { duration: 'total_days_supply',  gap: 'btn_era_gap_days' },
    allera: { duration: 'total_days_supply',  gap: 'btn_era_gap_days' },
  };
  let agg = aggs[bundle][measurename];

  let first_exp = ({duration: 1, gap: 2, overlap: 2})[measurename];

  let bundleCol = ({ exp: 'exp_num', era: 'era_num', allexp: null, allera: null })[bundle];

  if (!bundleCol) { // intentional weird indenting here so sql indenting comes
                    // out readable
    let crossNums = `
      /* ntileCross crossNums cn */
      select generate_series as cn_ntile
      from generate_series(1,@ntiles)
      `;
    let withEmptyNtiles = `
    /* ntileCross withEmptyNtiles wen */
    select cn.*, dern.*
    from (${crossNums}
    ) cn
    left outer join (${sql}) dern
      on cn.cn_ntile = dern.ntile `;
    sql = `
  /* ntileCross(${JSON.stringify(p)}) */
  select  '${agg}' as aggField,
          '${measurename}' as measurename,
          cn_ntile as ntile,
          count(${agg}) as count,
          min(${agg}), max(${agg}), avg(${agg})
  from (${withEmptyNtiles}
  ) wen
  group by 1,2,3
  order by 2,3 `;
    return sql;
  }
  let max_exp = `
        /* ntileCross max_exp */
        select max(${bundleCol}) 
        from (${sql}
        ) s `;
  let crossNums = `
      /* ntileCross crossNums cn */
      select generate_series as cn_${bundleCol}, b.ntile as cn_ntile
      from generate_series(${first_exp}, (${max_exp}
      ))
      join (select generate_series as ntile 
            from generate_series(1,@ntiles)) b on 1=1
      `;
  let withEmptyNtiles = `
    /* ntileCross withEmptyNtiles wen */
    select cn.*, dern.*
    from (${crossNums}
    ) cn
    left outer join (${sql}) dern
      on cn.cn_ntile = dern.ntile and cn.cn_${bundleCol} = dern.${bundleCol} `;
  sql = `
  /* ntileCross(${JSON.stringify(p)}) */
  select  '${agg}' as aggField,
          '${measurename}' as measurename,
          cn_${bundleCol} as ${bundleCol},
          cn_ntile as ntile,
          count(${bundleCol}) as count,
          min(${agg}), max(${agg}), avg(${agg})
  from (${withEmptyNtiles}
  ) wen
  group by 1,2,3,4
  order by 3,4 `;
  return sql;
}

function plainEras(p) {
  let {resultsSchema, maxgap, concept_id, person_id, ntiles, measurename, bundle, filter} = p;
  return `
            /* plainEras(${JSON.stringify(p)}) */
            select
                    person_id,
                    era_num,
                    count(*) as exposures,
                    min(era_days) as era_days, /* same in every value, so min,max,avg, doesn't matter */
                    /*max(drug_exposure_end) - min(drug_exposure_start_date) as era_days, */
                    sum(days_supply) as total_days_supply,
                    min(btn_era_gap_days) as btn_era_gap_days,
                    min(from_exp) as from_exp,
                    min(to_exp) as to_exp,
                    min(era_start_date) as era_start_date,
                    min(era_end_date) as era_end_date,
                    min(days_from_first_era) as days_from_first_era,
                    rollup_concept_id
            from (
              ${expPlusEraStats(p)}
            ) exp_w_era_stats
            ${filter||''}
            group by person_id, rollup_concept_id, era_num
                          `;
}
function expPlusEraStats(p) {
  return `
                /* expPlusEraStats(${JSON.stringify(p)}) */
                select
                        max(drug_exposure_end) over (partition by person_id, era_num) - min(drug_exposure_start_date) over (partition by person_id, era_num) as era_days,
                        first_value(days_from_latest) over (partition by person_id, era_num order by exp_num) as btn_era_gap_days,
                        min(exp_num) over (partition by person_id, era_num) from_exp,
                        max(exp_num) over (partition by person_id, era_num) to_exp,
                        first_value(drug_exposure_start_date) over (partition by person_id, era_num order by exp_num) era_start_date,
                        last_value(drug_exposure_end) over (partition by person_id, era_num order by exp_num) era_end_date,
                        first_value(drug_exposure_start_date) over (partition by person_id, era_num order by exp_num) -
                        first_value(drug_exposure_start_date) over (partition by person_id order by exp_num) as days_from_first_era,
                        exp_w_era_num.*
                from ( ${expPlusEraNum(p)}
                ) exp_w_era_num `;
}
function expPlusEraNum(p) {
  let {resultsSchema, maxgap, concept_id, person_id, 

          // ntiles, measurename,  
          // DON'T WANT TO SEND THESE FORWARD, RIGHT?
          // MESSES UP INNER exposure_rollup WITH UNNEEDED NTILES
    //
          bundle, entityName, filter} = p;
  let p2 = {resultsSchema, maxgap, concept_id, person_id, bundle, entityName, filter};
  return `
                    /* expPlusEraNums(${JSON.stringify(p2)}) */
                    select sum(case when exp_num = 1 or days_from_latest > @maxgap then 1 else 0 end)
                                over (partition by person_id order by exp_num
                                      rows between unbounded preceding and current row)
                                  as era_num,
                          exp_plus_era_num.*
                    from (${exposure_rollup(p2, false)}
                    ) exp_plus_era_num `;
}
function eras(p) {
  let {resultsSchema, maxgap, concept_id, person_id, ntiles, measurename, bundle, entityName} = p;
  let sql = plainEras({resultsSchema, maxgap, concept_id, person_id, ntiles, measurename, bundle, entityName});
  let where = [];

  let ntilecol =
        (typeof ntiles === 'undefined' || typeof measurename === 'undefined')
        ? ''
        : ntileCol({ntiles, measurename, bundle, entityName}, where);

  let whereClause = '';
  if (where.length) {
    whereClause = `where ${where.join(' and ')}`;
  }
  return `
          /* eras(${JSON.stringify(p)}) */
          select ${ntilecol}
                  plain_eras.*
          from (${sql}
          ) plain_eras
          ${whereClause} `;
}

var _cacheDirty = true; // because api server restarted

function where(filters) { return filters.length
                            ? ` where ${filters.join(' and ')} `
                            : '';}
function andItems(filters) { return filters.length
                                ? ` (${filters.join(' and ')}) `
                                : '';}
function orItems(filters) { return filters.length
                                ? ` (${filters.join(' or ')}) `
                                : '';}
function cacheDirty(cdm) {
  var returns = { arg: 'data', type: ['cdm'], root: true };
  cdm.cacheDirty = function(cb) {
    cb(null, _cacheDirty);
    _cacheDirty = false;
  };
  cdm.remoteMethod('cacheDirty', {
    accepts: [],
    returns,
    accessType: 'READ',
    http: { verb: 'get' }
  });
}
function toNamedParams(p, accepts) {
  let plist = p.slice(0);
  let params = {};
  accepts.forEach(arg => {
    let val = plist.shift();
    if (typeof val === 'undefined' || val === 'undefined')
      val = arg.default;
    if (arg.validCheck) {
      if (! arg.validCheck(val)) {
        throw new Error(`invalid value: ${val} for ${JSON.stringify(arg,null,2)}`);
      }
    }
    params[arg.arg] = val;
  })
  let req = params.req;
  delete params.req;
  return [params,req];
}

/*
console.log(Object.keys(loopback));

var memory = loopback.createDataSource({
connector: loopback.Memory,
//file: "mydata.json"
});
var MemModel = loopback.PersistedModel.extend('var MemModel');
MemModel.setup = function() {
var MemModel = this;

var returns = { arg: 'data', type: ['cdm'], root: true };
cdm.saveGet = cdm.savePost = 
  function(key, val, cb) {
    var ds = MemModel.dataSource;

    console.log('==============>\n', key, val, '\n<==============\n');
    ds.connector.query(sql, [], function(err, rows) {
      if (err) console.error(err);
      //console.log(Object.keys(rows));
      cb(err, rows.slice(0,1000));
    });
  };

var conceptsAccepts = [
    {arg: 'cdmSchema', type: 'string', required: true },
    {arg: 'resultsSchema', type: 'string', required: true},
    {arg: 'fullInfo', type: 'boolean', required: false, default: false},
    {arg: 'query', type: 'string', required: false},
    {arg: 'queryName', type: 'string', required: false, default: 'All concept stats'},
];

cdm.remoteMethod('conceptsGet', {
  accepts: conceptsAccepts,
  returns,
  accessType: 'READ',
  http: {
    verb: 'get'
  }
});
cdm.remoteMethod('conceptsPost', {
  accepts: conceptsAccepts,
  returns,
});

}
*/
/*
function conceptsOLD(cdm) {
  var returns = { arg: 'data', type: ['cdm'], root: true };

  const where = (filters) => filters.length
                              ? ` where ${filters.join(' and ')} `
                              : '';

  cdm.concepts = 
    function(cdmSchema, resultsSchema, attr, 
             excludeInvalidConcepts, excludeNoMatchingConcepts, excludeNonStandardConcepts,
             query, queryName, cb) {
      var ds = cdm.dataSource;
      let allParams = {
            cdmSchema, resultsSchema, 
            excludeInvalidConcepts, excludeNoMatchingConcepts, excludeNonStandardConcepts,
            query, queryName,
      }

      let sql = '';
      query = query || queryName;
      let filters = [];
      switch (query) {
        case 'conceptStats':
          if (attr) {
            if (excludeInvalidConcepts) filters.push('invalid_reason is null');
            if (excludeNoMatchingConcepts) filters.push('concept_id != 0');
            if (excludeNonStandardConcepts) filters.push('standard_concept is not null');
            sql = `
                    select ${attr}, sum(count) as dbrecs, count(*) as conceptrecs
                    from ${resultsSchema}.concept_info
                    ${where(filters)}
                    group by 1`;
          } else {
            if (excludeInvalidConcepts) filters.push('invalid = false');
            if (excludeNoMatchingConcepts) filters.push(`vocabulary_id != 'None'`);
            if (excludeNonStandardConcepts) filters.push('standard_concept is not null');
            sql = `
                    select *
                    from ${resultsSchema}.concept_info_stats
                    ${where(filters)}
                  `;
          }
          break;
        case 'conceptCount':
          if (excludeInvalidConcepts) filters.push('invalid_reason is null');
          if (excludeNoMatchingConcepts) filters.push('concept_id != 0');
          if (excludeNonStandardConcepts) filters.push('standard_concept is not null');
          sql = `
                  select count(*) as count
                  from ${cdmSchema}.concept
                  ${where(filters)}
                `;
          break;
        case 'classRelations':
          if (excludeInvalidConcepts) filters.push(`invalid_1 = false`, `invalid_2 = false`);
          if (excludeNoMatchingConcepts) filters.push(`vocab_1 != 'None'`, `vocab_2 != 'None'`);
          if (excludeNonStandardConcepts) filters.push('sc_1 is not null', 'sc_2 is not null');
          sql =  `
                  select * 
                  from ${resultsSchema}.class_relations 
                  ${where(filters)}
                  order by 1,2,5,6,11,8,9,10,16,13,14,15`;
      }
      console.log('==============>\nRequest:\n', allParams, sql, '\n<==============\n');
      ds.connector.query(sql, [], function(err, rows) {
        if (err) {
          console.error(err);
          cb(err, []);
        } else {
          console.log('==============>\nResponse:\n', allParams, sql, `${rows.length} rows`, '\n<==============\n');
          console.warn("TRUNCATING TO 1000 ROWS!!! FIX THIS (with pagination?)!!!");
          cb(err, rows.slice(0,1000));
        }
      });
    };

  var conceptsAccepts = [
      {arg: 'cdmSchema', type: 'string', required: true },
      {arg: 'resultsSchema', type: 'string', required: true},
      {arg: 'attr', type: 'string', required: false},
      {arg: 'excludeInvalidConcepts', type: 'boolean', required: false, default: true},
      {arg: 'excludeNoMatchingConcepts', type: 'boolean', required: false, default: true},
      {arg: 'excludeNonStandardConcepts', type: 'boolean', required: false, default: false},
      //{arg: 'fullInfo', type: 'boolean', required: false, default: false},
      {arg: 'query', type: 'string', required: true},
      {arg: 'queryName', type: 'string', required: false, default: 'All concept stats'},
  ];

  cdm.remoteMethod('concepts', {
    accepts: conceptsAccepts,
    returns,
    accessType: 'READ',
    http: {
      verb: 'get'
    }
  });
  cdm.remoteMethod('concepts', {
    accepts: conceptsAccepts,
    returns,
  });
}
  function drugConceptSql(params, flavor) {
    let filters = filterConditions(params);
    let cols =  `
                  coalesce(sum(dcc.count),0) AS exposure_count,
                  count(*) AS concept_count
                `;
    let groupBy = '';
    switch (flavor) {
      case 'counts':
        break;  // just the counts
      case 'target':
        cols = `
                  c.concept_name AS concept_name,
                  ct.concept_name AS type_concept_name,
                  c.invalid_reason, 
                  c.standard_concept, 
                  c.domain_id, 
                  c.vocabulary_id, 
                  c.concept_class_id,
                  dcc.drug_concept_id AS concept_id,
                  dcc.drug_type_concept_id AS type_concept_id,
                ` + cols;
        break;
      case 'source':
        throw new Error("not handling yet");
      case 'target_agg':
        cols = `
                  ct.concept_name AS type_concept_name,
                  c.invalid_reason, 
                  c.standard_concept, 
                  c.domain_id, 
                  c.vocabulary_id, 
                  c.concept_class_id,
                ` + cols;
        groupBy = `
                group by ${_.range(1, 7)}
                  `;
        break;
    }
    let sql = `
          SELECT  ${cols}
          FROM ${params.resultsSchema}.drug_concept_counts dcc
          JOIN ${params.cdmSchema}.concept c ON dcc.drug_concept_id = c.concept_id
          JOIN ${params.cdmSchema}.concept ct on dcc.drug_type_concept_id = ct.concept_id and ct.invalid_reason is null
          ${where(filters)}
          ${groupBy}
        `;
    return sql;
  }

  cdm.drugConceptAgg = function(..._params) {
    var accepts = [].concat(schemaArgs, filterArgs, otherArgs);
    const cb = _params.pop();
    let params = toNamedParams(_params, accepts);
    let sql = drugConceptSql(params, 'target_agg');
    runQuery(cdm, cb, req, sql, params);
  };
  cdm.remoteMethod('drugConceptAgg', { accepts, returns, accessType: 'READ', http: { verb: 'post' } });
  cdm.remoteMethod('drugConceptAgg', { accepts, returns, accessType: 'READ', http: { verb: 'get' } });

  cdm.drugConceptCounts = function(..._params) {
    var accepts = [].concat(schemaArgs, filterArgs, otherArgs);
    const cb = _params.pop();
    let params = toNamedParams(_params, accepts);
    let sql = drugConceptSql(params, 'counts');
    runQuery(cdm, cb, req, sql, params);
  };
  cdm.remoteMethod('drugConceptCounts', { accepts, returns, accessType: 'READ', http: { verb: 'post' } });
  cdm.remoteMethod('drugConceptCounts', { accepts, returns, accessType: 'READ', http: { verb: 'get' } });

  cdm.classRelations = function(..._params) {
    var accepts = [].concat(schemaArgs, filterArgs, otherArgs);
    const cb = _params.pop();
    let params = toNamedParams(_params, classAccepts);
    let domainFilt = '';
    if (params.domain_id) {
       //['Drug','Condition'].indexOf(params.domain_id) > -1  // checking elsewhere
      // or maybe not yet, but should be
      domainFilt = ` and domain_1='${params.domain_id}' and domain_2='${params.domain_id}' `;
    }
    let hierFilt = '';
    switch (params.hierarchical) {
      case 'is_hierarchical':
        hierFilt = ` and is_hierarchical = '1' `;
        break;
      case 'defines_ancestry':
        hierFilt = ` and defines_ancestry = '1' `;
        break;
      case 'both':
        hierFilt = ` and is_hierarchical = '1' and defines_ancestry = '1' `;
        break;
      case 'either':
        hierFilt = ` and (is_hierarchical = '1' or defines_ancestry = '1') `;
        break;
      case 'neither':
        hierFilt = ` and is_hierarchical != '1' and defines_ancestry != '1'`;
        break;
    }

    let sql = `
                select
                        is_hierarchical,
                        defines_ancestry,
                        same_vocab,
                        sc_1,
                        sc_2,
                        vocab_1,
                        vocab_2,
                        class_1,
                        class_2,
                        relationship_id,
                        sum(c1_ids) c1_ids,
                        sum(c2_ids) c2_ids,
                        sum(c) c
                from ${params.resultsSchema}.class_relations
                where invalid_1=false and invalid_2=false
                  ${hierFilt}
                  ${domainFilt}
                  --and sc_2 is not null -- is this the right thing to do?
                group by 1,2,3,4,5,6,7,8,9,10
                order by 1,2,3,4,5,6,7,8,9,10
              `;
    runQuery(cdm, cb, req, sql, params);
  };
  cdm.remoteMethod('classRelations', { accepts:classAccepts, returns, accessType: 'READ', http: { verb: 'post' } });
  cdm.remoteMethod('classRelations', { accepts:classAccepts, returns, accessType: 'READ', http: { verb: 'get' } });



*/


function runQuery(cdm, cb, req, sql, params, rowTransform=d=>d, logRequest, 
                  apiName='missingApiName', qpFunc) {
  var ds = cdm.dataSource;
  params = _.clone(params);
  //console.log('URLs', cdm.app.get('url'), req.url,  req.baseUrl, req.originalUrl, req._parsedUrl, req.params, req.query);
  //console.log(apiName, params, sql);
  let queryParams = qpFunc ? qpFunc(params) : [];
  var url = `\nurl ---> ${cdm.app.get('url').replace(/\/$/, '') + req.originalUrl}\n`;
  if (logRequest)
    console.log('==============>\nRequest:\n', apiName, params, sql, url, '\n<==============\n');
  if (cb) {
    console.log('in runQuery, cb is', typeof cb, cb);
    return ds.connector.query(sql, queryParams, function(err, rows) { 
      if (err) {
        console.log("calling cb with query error", err);
        cb(err, null);
        return;
      }
      logResponse(null, err, rows, apiName, sql, url, params, rowTransform, logRequest, queryParams);
      if (!Array.isArray(rows)) {
        console.error("how could this happen?", rows);
      }
      let result = rows.map(rowTransform);
      cb(err, result);
    });
  }
  return new Promise(function(resolve, reject) {
    console.log('promise for', apiName);
    return ds.connector.query(sql, queryParams, function(err, rows) { 
      //console.log('finished query');
      logResponse(null, err, rows, apiName, sql, url, params, rowTransform, logRequest, queryParams);
      if (err) {
        console.log("query error in promise", err);
        reject({err, rows, url, cdm, sql, params, rowTransform, logRequest, apiName});
        return;
      }
      //console.log('resolving', apiName, 'with', err,rows.length);
      if (!Array.isArray(rows)) {
        console.error("how could this happen?", rows);
      }
      let result = rows.map(rowTransform);
      resolve({err, rows:result, url, cdm, sql, params, rowTransform, logRequest, apiName});
    });
  });
}
function logResponse(cb, err, rows, apiName, sql, url, params, rowTransform, logRequest, queryParams) {
  sql = sql.replace(/\$(\d+)/g, (match,p1)=>queryParams[p1-1]);
  if (err) {
    console.error('==============>\nRequest Error:\n', err, params, sql, 
                      url, '\n<==============\n');
  } else {
    console.log('==============>\nResponse:\n', 
                {apiName, params, queryParams, rows:rows.length, }, 
                url, sql, '\n<==============\n');
  }
}
function multipleResultSets(cb, promises) {
  return Promise.all(promises).then(
    resultSets=>{
      let allResults = _.fromPairs(
        resultSets.map(
          ({err, rows, apiName, url, cdm, sql, params, })=>{
            if (err) {
              console.error('problem with resultSet in ', apiName, url);
            } else {
              return [apiName,rows];
            }
          }));
      cb(null, allResults);
    },
    (err,...other) => {
      console.error('ERROR', err, other);
    }
  );
}

function generateRemoteMethod({apiName, cdm, accepts, returns, 
                              sqlTemplate, rowTransform,
                              resultsTransform, qpFunc}={}) {
  cdm[apiName] = function(..._params) {
    let source, params, cb, req, resultsOnly=true;
    if (_params[0] === 'calledFromMethod') {
      //console.log("got calledFromMethod");
      if (!resultsOnly) throw new Error("can't remember what resultsOnly is for", _params);
      [source, params, cb, req, resultsOnly] = _params;
      //console.log({source, params, cb, resultsOnly});
      //console.log("got object _params", _.keys(params), 'for', apiName);
    } else {
      cb = _params.slice(_params.length-1)[0];
      [params,req] = toNamedParams(_params.slice(0,_params.length-1), accepts);
      //console.log("got ", _params.length, _params.map(d=>typeof d), "list _params; after naming:\n", _.keys(params));
    }
    let queryError;
    let promise = runQuery(cdm, null, req, sqlTemplate(params), params, 
                            rowTransform, false, apiName,
                            qpFunc)
                    .then(p => {
                            //console.log("in genrem promise then", apiName, 'result', p)
                            return resultsOnly ? (p.rows || null) : p;
                          },
                          ({err, rows} = {}) => {
                            console.error("genRemMeth got problem back from runQuery", err, rest);
                            return err;
                            //cb(err, rows);
                            queryError = err;
                          });
    if (queryError) {
      console.log("weird stuff");
    }

    if (resultsTransform) {
      return promise.then(
        p => {
          //console.log('resultsTransform got ', p.rows || p);
          return resultsTransform(p.rows || p);
        });
    }
    return promise;
  }
  cdm.remoteMethod(apiName, {accepts, returns, accessType: 'READ', http: { verb: 'post' } });
  cdm.remoteMethod(apiName, {accepts, returns, accessType: 'READ', http: { verb: 'get' } });
}

function rowLimit(p, limit) {
  let {err, rows, apiName, url, cdm, sql, params, } = p;
  return {err, rows: rows && rows.slice(0,limit), 
          apiName, url, cdm, sql, params, };
}

