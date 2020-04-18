#-----------------------------------------------------------------------
#
# This file is part of osmquadtreepostgis
#
# Copyright (C) 2019 James Harris
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
#-----------------------------------------------------------------------


from __future__ import print_function
import oqt
from . import _osmquadtreepostgis as opg

import json, psycopg2,csv
from oqt.utils import addto, Prog, replace_ws, addto_merge
from oqt.pbfformat import get_locs
import time,sys,re, gzip

from oqt.geometry import style as geometrystyle, minzoomvalues, process

default_extra_node_cols = ['access','addr:housename','addr:housenumber','addr:interpolation','admin_level','bicycle','covered','foot','horse','layer','name','oneway','ref','religion','surface']
default_extra_way_cols = ['addr:housenumber', 'admin_level', 'layer', 'bicycle', 'name', 'tracktype', 'addr:interpolation', 'addr:housename', 'horse', 'surface', 'access', 'religion', 'oneway', 'foot', 'covered', 'ref']


def postgis_columns(style, add_min_zoom, extended=False, extra_node_cols=None, extra_way_cols=None):
    ans = []
    
    node_cols = set(style.feature_keys)
    way_cols = set(style.feature_keys)
    if style.other_keys is None:
        node_cols.update(extra_node_cols if not extra_node_cols is None else default_extra_node_cols)
        way_cols.update(extra_way_cols if not extra_way_cols is None else default_extra_way_cols)
    else:
        node_cols.update(style.other_keys)
        way_cols.update(style.other_keys)
    
        
    point_cols = [
        opg.GeometryColumnSpec("osm_id", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.OsmId),
        opg.GeometryColumnSpec("quadtree", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.ObjectQuadtree),
        opg.GeometryColumnSpec("tile", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.BlockQuadtree),
    ]
    
    point_cols += [opg.GeometryColumnSpec(k, opg.GeometryColumnType.Text, opg.GeometryColumnSource.Tag) for k in sorted(node_cols)]
    point_cols += [opg.GeometryColumnSpec(k, opg.GeometryColumnType.Text, opg.GeometryColumnSource.Tag) for k in style.parent_tags]
    
    if add_min_zoom:
        point_cols.append(opg.GeometryColumnSpec('minzoom', opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.MinZoom))
    if style.other_keys is None:
        point_cols.append(opg.GeometryColumnSpec('tags', opg.GeometryColumnType.Hstore, opg.GeometryColumnSource.OtherTags))
    point_cols.append(opg.GeometryColumnSpec('way', opg.GeometryColumnType.PointGeometry, opg.GeometryColumnSource.Geometry))
    
    line_cols = [
        opg.GeometryColumnSpec("osm_id", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.OsmId),
        opg.GeometryColumnSpec("quadtree", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.ObjectQuadtree),
        opg.GeometryColumnSpec("tile", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.BlockQuadtree),
    ]
    line_cols += [opg.GeometryColumnSpec(k, opg.GeometryColumnType.Text, opg.GeometryColumnSource.Tag) for k in sorted(way_cols) if k!='layer']
    line_cols += [opg.GeometryColumnSpec(k.target_key, opg.GeometryColumnType.Text, opg.GeometryColumnSource.Tag) for k in style.relation_tag_spec]
    
    line_cols.append(opg.GeometryColumnSpec("layer", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.Layer))
    line_cols.append(opg.GeometryColumnSpec("z_order", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.ZOrder))
    
    if add_min_zoom:
        line_cols.append(opg.GeometryColumnSpec('minzoom', opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.MinZoom))
    if style.other_keys is None:
        line_cols.append(opg.GeometryColumnSpec('tags', opg.GeometryColumnType.Hstore, opg.GeometryColumnSource.OtherTags))
    
    line_cols.append(opg.GeometryColumnSpec('length', opg.GeometryColumnType.Double, opg.GeometryColumnSource.Length))
    line_cols.append(opg.GeometryColumnSpec('way', opg.GeometryColumnType.LineGeometry, opg.GeometryColumnSource.Geometry))
    
    poly_cols = [
        opg.GeometryColumnSpec("osm_id", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.OsmId),
        opg.GeometryColumnSpec("quadtree", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.ObjectQuadtree),
        opg.GeometryColumnSpec("tile", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.BlockQuadtree),
    ]
    
    poly_cols += [opg.GeometryColumnSpec(k, opg.GeometryColumnType.Text, opg.GeometryColumnSource.Tag) for k in sorted(way_cols) if k!='layer']
    
    poly_cols.append(opg.GeometryColumnSpec("layer", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.Layer))
    poly_cols.append(opg.GeometryColumnSpec("z_order", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.ZOrder))
    
    if add_min_zoom:
        poly_cols.append(opg.GeometryColumnSpec('minzoom', opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.MinZoom))
    if style.other_keys is None:
        poly_cols.append(opg.GeometryColumnSpec('tags', opg.GeometryColumnType.Hstore, opg.GeometryColumnSource.OtherTags))
    poly_cols.append(opg.GeometryColumnSpec('way_area', opg.GeometryColumnType.Double, opg.GeometryColumnSource.Area))
    poly_cols.append(opg.GeometryColumnSpec('way', opg.GeometryColumnType.Geometry, opg.GeometryColumnSource.Geometry))
    
    poly_cols.append(opg.GeometryColumnSpec('way_point', opg.GeometryColumnType.PointGeometry, opg.GeometryColumnSource.RepresentativePointGeometry))
        
    point = opg.GeometryTableSpec("point")
    point.set_columns(point_cols)
    
    line = opg.GeometryTableSpec("line")
    line.set_columns(line_cols)
    
    polygon = opg.GeometryTableSpec("polygon")
    polygon.set_columns(poly_cols)
    
    
    highway=opg.GeometryTableSpec('highway')
    highway.set_columns(line_cols)
    
    building=opg.GeometryTableSpec('building')
    building.set_columns(poly_cols[:-1])
    
    boundary=opg.GeometryTableSpec('boundary')
    boundary.set_columns([p for p in poly_cols if p.name in ('osm_id','part','quadtree','tile','boundary','admin_level','name','ref', 'minzoom','way_area','way')])
    return [point,line,polygon,highway,building,boundary]
    

    
    
    
def type_str(ct):
    if ct==opg.GeometryColumnType.BigInteger:
        return 'bigint'
    elif ct==opg.GeometryColumnType.Text:
        return 'text'
    elif ct==opg.GeometryColumnType.Double:
        return 'float'
    elif ct==opg.GeometryColumnType.Hstore:
        return 'hstore'
    elif ct==opg.GeometryColumnType.Geometry:
        return 'geometry(Geometry,3857)'
    elif ct==opg.GeometryColumnType.PointGeometry:
        return 'geometry(Point,3857)'
    elif ct==opg.GeometryColumnType.LineGeometry:
        return 'geometry(Linestring,3857)'
    elif ct==opg.GeometryColumnType.PolygonGeometry:
        return 'geometry(Polygon,3857)'
    raise Exception("unexpected column type "+repr(ct))
    
def prep_table_create(prfx, ts):
    
    cols = [(c.name, type_str(c.type)) for c in ts.columns]
    return 'create table %s%s (%s) with oids' % (prfx, ts.table_name, ", ".join('"%s" %s' % (a,b) for a,b in cols))
    
    

def drop_tables(curs, table_prfx):
    for t in ('point','line','polygon','roads','boundary','building','highway'):
        curs.execute("drop table if exists "+table_prfx+t+" cascade" )
    
    
    
def create_tables(curs, table_prfx,coltags):
    drop_tables(curs, table_prfx)
    
    for ct in coltags:
        create = prep_table_create(table_prfx, ct)
        print(create)
        curs.execute(create)
        x="alter table %s%s set (autovacuum_enabled=false)" % (table_prfx, ct.table_name)
        print(x)
        curs.execute(x)
    return
    
    



            
def write_indices(curs,table_prfx, inds):
    ist=time.time()
    
    for ii in inds:
        qu=ii.replace("%ZZ%", table_prfx)
        sys.stdout.write("%-150.150s" % (replace_ws(qu),))
        sys.stdout.flush()
        qst=time.time()
        
        curs.execute(qu)
        
        sys.stdout.write(" %7.1fs\n" % (time.time()-qst))
        sys.stdout.flush()
    
    print("created indices in %8.1fs" % (time.time()-ist))


planetosm = [
"drop view if exists planet_osm_point",
"drop view if exists planet_osm_line",
"drop view if exists planet_osm_polygon",
"drop table if exists planet_osm_roads",
"drop view if exists planet_osm_highway",
"drop view if exists planet_osm_building",
"drop view if exists planet_osm_boundary",
"drop view if exists planet_osm_polygon_point",
"create view planet_osm_point as (select * from %ZZ%point)",
"create view planet_osm_line as (select * from %ZZ%line union all select * from %ZZ%highway)",
#"create view planet_osm_polygon as (select * from %ZZ%polygon union all select * from %ZZ%building)",
"""create table planet_osm_roads as (
    SELECT osm_id,tile,quadtree,name,ref,admin_level,highway,railway,boundary,
            service,tunnel,bridge,z_order,covered,surface, minzoom, way
        FROM %ZZ%highway
        WHERE highway in (
            'secondary','secondary_link','primary','primary_link',
            'trunk','trunk_link','motorway','motorway_link')
        OR railway is not null

    UNION ALL
    
    SELECT osm_id,tile,quadtree,name,null as ref, admin_level,null as highway,
            null as railway, boundary, null as service,
            null as tunnel,null as bridge, 0  as z_order,null as covered,null as surface,minzoom, 
            st_exteriorring(way) as way
        FROM %ZZ%boundary WHERE
            osm_id<0 and boundary='administrative')""",

"create index planet_osm_roads_way_admin on planet_osm_roads using gist(way) where (osm_id < 0 and boundary='administrative')",
"create index planet_osm_roads_way_highway on planet_osm_roads using gist(way) where (highway in ('secondary','secondary_link','primary','primary_link','trunk','trunk_link','motorway','motorway_link') OR railway is not null)",
"vacuum analyze planet_osm_roads",
"create view planet_osm_highway as (select * from %ZZ%highway)",
"create view planet_osm_building as (select * from %ZZ%building)",
"create view planet_osm_boundary as (select * from %ZZ%boundary)",
"create view planet_osm_polygon_point as select * from %ZZ%polygon_point",

]

extended_indices_pointline = [
"create index %ZZ%point_way on %ZZ%point using gist(way)",
"create index %ZZ%line_way on %ZZ%line using gist(way)",
"create index %ZZ%highway_way on %ZZ%highway using gist(way)",
"""create index %ZZ%highway_way_lz on %ZZ%highway using gist(way) where (
    highway in ('motorway','motorway_link','trunk','trunk_link','primary','primary_link','secondary')
    or (railway in ('rail','light_rail','narrow_gauge','funicular') and (service IS NULL OR service NOT IN ('spur', 'siding', 'yard')))
)""",

"create index %ZZ%point_id on %ZZ%point using btree(osm_id)",
"create index %ZZ%line_id on %ZZ%line using btree(osm_id)",
"create index %ZZ%highway_id on %ZZ%highway using btree(osm_id)",

"create index %ZZ%point_name on %ZZ%point using gin(name gin_trgm_ops)",
"create index %ZZ%line_name on %ZZ%line using gin(name gin_trgm_ops)",
#"create index %ZZ%highway_name on %ZZ%highway using gin(name gin_trgm_ops)",

"vacuum analyze %ZZ%point",
"vacuum analyze %ZZ%line",
"vacuum analyze %ZZ%highway",

"create view %ZZ%json_point as select osm_id,quadtree,tile,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'quadtree' - 'tile' - 'tags' - 'minzoom' - 'way') || tags::jsonb as properties, minzoom, way from %ZZ%point pp",
"create view %ZZ%json_line as select osm_id,quadtree,tile,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'quadtree' - 'tile' - 'tags' - 'z_order' - 'minzoom' - 'way') || tags::jsonb as properties, z_order, minzoom, way from %ZZ%line pp",
"create view %ZZ%json_highway as select osm_id,quadtree,tile,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'quadtree' - 'tile' - 'tags' - 'z_order' - 'minzoom' - 'way') || tags::jsonb as properties, z_order, minzoom, way from %ZZ%highway pp",

"alter table %ZZ%point set (autovacuum_enabled=true)",
"alter table %ZZ%line set (autovacuum_enabled=true)",
"alter table %ZZ%highway set (autovacuum_enabled=true)",

]

extended_indices_polygon = [
"create index %ZZ%polygon_way on %ZZ%polygon using gist(way)",
"create index %ZZ%building_way on %ZZ%building using gist(way)",
"create index %ZZ%boundary_way on %ZZ%boundary using gist(way)",
"create index %ZZ%polygon_way_point on %ZZ%polygon using gist(way_point) where way_point is not null",

"create index %ZZ%polygon_id on %ZZ%polygon using btree(osm_id)",
"create index %ZZ%building_id on %ZZ%building using btree(osm_id)",
"create index %ZZ%boundary_id on %ZZ%boundary using btree(osm_id)",

"create index %ZZ%polygon_name on %ZZ%polygon using gin(name gin_trgm_ops)",
"create index %ZZ%boundary_name on %ZZ%boundary using gin(name gin_trgm_ops)",

"vacuum analyze %ZZ%polygon",
"vacuum analyze %ZZ%building",
"vacuum analyze %ZZ%boundary",

"create view %ZZ%json_polygon as select osm_id,quadtree,tile,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'quadtree' - 'tile' - 'tags' - 'z_order' - 'minzoom' - 'way_area' - 'way' - 'way_point') || tags::jsonb as properties, z_order, minzoom, way_area, way, way_point from %ZZ%polygon pp",
"create view %ZZ%json_building as select osm_id,quadtree,tile,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'quadtree' - 'tile' - 'tags' - 'z_order' - 'minzoom' - 'way_area' - 'way') || tags::jsonb as properties, z_order, minzoom, way_area, way from %ZZ%building pp",
"create view %ZZ%json_boundary as select osm_id,quadtree,tile,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'quadtree' - 'tile' - 'minzoom' - 'way_area' - 'way') as properties, minzoom, way_area, way from %ZZ%boundary pp",
"create view %ZZ%polygon_exterior as select * from %ZZ%polygon",

"alter table %ZZ%polygon set (autovacuum_enabled=true)",
"alter table %ZZ%building set (autovacuum_enabled=true)",
"alter table %ZZ%boundary set (autovacuum_enabled=true)",
]

def find_polygon_cols(curs, table_prfx,skip=set(['way','way_point'])):
    curs.execute("select * from %spolygon limit 0" % table_prfx)
    return ", ".join('"%s"' % c[0] for c in curs.description if not c[0] in skip)

    

def write_extended_indices_pointline(curs, table_prfx):
    
    write_indices(curs, table_prfx, extended_indices_pointline)
    
def write_extended_indices_polygon(curs, table_prfx):
    
    poly_cols = find_polygon_cols(curs, table_prfx)
    
    inds = extended_indices_polygon[:]    
    inds.append("create view %ZZ%polygon_point as select "+poly_cols+", way_point as way from %ZZ%polygon where way_point is not null")
    
    write_indices(curs, table_prfx, inds)
    
def write_planetosm_views(curs, table_prfx):
    inds = planetosm[:]
    roadspp = min(i for i,ind in enumerate(inds) if not 'drop' in ind and 'planet_osm_roads' in ind)
    
    poly_cols = find_polygon_cols(curs, table_prfx,set(['way_point']))
    
    inds.insert(roadspp, "create view planet_osm_polygon as (select "+poly_cols+" from %ZZ%polygon union all select "+poly_cols+" from %ZZ%building)")
    
    write_indices(curs, table_prfx, inds)

def create_tables_lowzoom(curs, prfx, newprefix, minzoom, simp=None, cols=None,table_names=None, polygonpoint=True):
    
    
    queries = []
    if table_names is None:
        table_names = ["point", "line", "polygon","boundary","highway","building"]
    
    for tab in table_names:
        queries.append("drop table if exists %ZZ%"+tab+" cascade")
    
    
    
    for tab in table_names:
    
        colsstr="*"
        if not simp is None:
            ncols=None            
            
            if cols is None:
                curs.execute("select * from "+prfx+tab+" limit 0")
                ncols=['"%s"'  % c[0] for c in curs.description if c[0]!='way']
            else:
                ncols=cols[:]
                
            if tab=='point':
                ncols.append('way')
            else:
                ncols.append("st_simplify(way, "+str(simp)+") as way")
            colsstr = ", ".join(ncols)
            
    
        queries.append("create table %ZZ%" + tab + " as "+\
             "(select "+colsstr+" from "+prfx+tab+\
             " where minzoom <= "+str(minzoom)+")")
             
    
    for tab in table_names:
        queries.append("create index %ZZ%" + tab +"_way on %ZZ%" + tab +" using gist(way)")
        queries.append("create index %ZZ%" + tab +"_id on %ZZ%" + tab +" using btree(osm_id)")
    
    if polygonpoint:
        cols=find_polygon_cols(curs, prfx)
        queries.append("create view %ZZ%polygon_point as select "+cols+", way_point as way from %ZZ%polygon where way_point is not null")
        queries.append("create index %ZZ%polygon_waypoint on %ZZ%polygon using gist(way_point) where way_point is not null")
    
        queries.append("create view %ZZ%polygon_exterior as select * from %ZZ%polygon")

    for tab in table_names:
        queries.append("vacuum analyze %ZZ%" + tab)
        
    
    print("call %d queries..." % len(queries))
    write_indices(curs,newprefix,queries)
    
    
def create_views_lowzoom(curs, prfx, newprefix, minzoom, indices=True, table_names=None):
    
    
    
    queries=[]
    if table_names is None:
        table_names=["point", "line", "polygon","highway", "building", "boundary","polygon_point","polygon_exterior"]
    
    for tab in table_names:
        queries.append("drop view if exists %ZZ%"+tab)
        queries.append("create view %ZZ%"+tab+" as (select * from "+prfx+tab+" where minzoom <= "+str(minzoom)+")")
        if indices and not '_' in tab:
            queries.append("drop index if exists %ZZ%"+tab+"_way")
            queries.append("create index %ZZ%"+tab+"_way on "+prfx+tab+" using gist(way) where minzoom <= "+str(minzoom))
    if indices and 'polygon_point' in table_names:
        queries.append("drop index if exists %ZZ%polygon_waypoint")
        queries.append("create index %ZZ%polygon_waypoint on "+prfx+"polygon using gist(way_point) where way_point is not null and minzoom <= "+str(minzoom))
    
    write_indices(curs,newprefix,queries)


def get_db_conn(connstring):
    conn=psycopg2.connect(connstring)
    conn.autocommit=True
    return conn

def write_to_postgis(prfx, box_in,connstr, tabprfx, stylefn=None, writeindices=True, lastdate=None,minzoom=None,nothread=False, numchan=4, minlen=0,minarea=5,use_binary=True):
    if not connstr or not tabprfx:
        raise Exception("must specify connstr and tabprfx")
        
        
    params,style = process.prep_geometry_params(prfx, box_in, stylefn, lastdate, minzoom, numchan, minlen, minarea)
    
    
    
    postgisparams = opg.PostgisParameters()
    
    #params.coltags = sorted((k,v.IsNode,v.IsWay,v.IsWay) for k,v in params.style.items() if k not in ('z_order','way_area'))
    postgisparams.coltags = postgis_columns(style, params.findmz is not None, extended=True)
    
    postgisparams.alloc_func='extended'
    postgisparams.validate_geometry = True
    
    if tabprfx and not tabprfx.endswith('_'):
        tabprfx = tabprfx+'_'
    postgisparams.tableprfx = tabprfx + ('_' if tabprfx and not tabprfx.endswith('_') else '')
    postgisparams.connstring = connstr
    postgisparams.use_binary=use_binary
    
    
    if postgisparams.connstring!='null':
        with get_db_conn(postgisparams.connstring) as conn:
            create_tables(conn.cursor(), postgisparams.tableprfx, postgisparams.coltags)
    
        
    cnt,errs=None,None
    if nothread:
        errs = opg.process_geometry_postgis_nothread(params, postgisparams, Prog(locs=params.locs))
    else:
        errs = opg.process_geometry_postgis(params, postgisparams, None)#Prog(locs=params.locs))
        
    if writeindices and postgisparams.connstring!='null':
        with get_db_conn(postgisparams.connstring) as conn:
            write_extended_indices_pointline(conn.cursor(), postgisparams.tableprfx)
            write_extended_indices_polygon(conn.cursor(), postgisparams.tableprfx)
        
    return errs

class CsvWriter:
    
    def __init__(self, outfnprfx,toobig=False):
        self.storeblocks=outfnprfx is None
        self.toobig=toobig
        
        
        self.outs={}
        self.num={}
        self.outfnprfx=outfnprfx
        if outfnprfx is None:
            self.blocks=[]
        
        
    
    def get_out(self, tab):
        if not tab in self.outs:
            self.outs[tab]=gzip.open("%s%s.csv.gz" % (self.outfnprfx,tab),'w',5)
            self.num[tab]=0
        return self.outs[tab]
    
    def __call__(self, block):
        if self.storeblocks:
            if block:
                if self.toobig:
                    self.blocks.append([(k,len(v),len(v.data())) for k,v in block.rows.items()])
                else:
                    self.blocks.append(block)
            return
        
        if not block :
            for k,v in self.outs.items():
                v.close()
            print("written: %s" % self.outs)
            return
        
        
        for k,v in block.rows.items():
            self.get_out(k).write(v.data())
            self.num[k] += len(v)
            
    
def write_to_csvfile(prfx, box_in,outfnprfx,  stylefn=None, lastdate=None,minzoom=None,nothread=False, numchan=4, minlen=0,minarea=5, use_binary=True):
    
    params,style = process.prep_geometry_params(prfx, box_in, stylefn, lastdate, minzoom, numchan, minlen, minarea)
    
    
    #params.coltags = sorted((k,v.IsNode,v.IsWay,v.IsWay) for k,v in params.style.items() if k not in ('z_order','way_area'))
    
    postgisparams=opg.PostgisParameters()
    postgisparams.coltags = postgis_columns(style, params.findmz is not None, extended)
    postgisparams.alloc_func='extended'
    postgisparams.use_binary=use_binary
    
    
    cnt,errs=None,None
    if nothread:
        csvwriter=CsvWriter(outfnprfx,outfnprfx is None and len(params.locs)>100)
        cnt, errs = opg.process_geometry_csvcallback_nothread(params, postgisparams, Prog(locs=params.locs), csvwriter)
        
    else:
        if outfnprfx is None:
            
            csvwriter=CsvWriter(outfnprfx,len(params.locs)>100)
            cnt, errs = opg.process_geometry_csvcallback(params, postgisparams, Prog(locs=params.locs),csvwriter)
        else:
            cnt, errs = opg.process_geometry_csvcallback_write(params, postgisparams, Prog(locs=params.locs),outfnprfx)
    #if writeindices:
    #    create_indices(psycopg2.connect(params.connstring).cursor(), params.tableprfx, extraindices, extraindices)

    if outfnprfx is None:
        return cnt, errs, csvwriter.blocks
    return cnt,errs
