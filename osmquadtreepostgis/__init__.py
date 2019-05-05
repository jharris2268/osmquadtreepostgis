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
        #opg.GeometryColumnSpec("part", opg.GeometryColumnType.BigInteger, opg.GeometryColumnSource.Part),
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
    
    if extended:
        poly_cols.append(opg.GeometryColumnSpec('way_point', opg.GeometryColumnType.PointGeometry, opg.GeometryColumnSource.RepresentativePointGeometry))
    
    
    point = opg.GeometryTableSpec("point")
    point.set_columns(point_cols)
    
    line = opg.GeometryTableSpec("line")
    line.set_columns(line_cols)
    
    polygon = opg.GeometryTableSpec("polygon")
    polygon.set_columns(poly_cols)
    
    if extended:
        highway=opg.GeometryTableSpec('highway')
        highway.set_columns(line_cols)
        
        building=opg.GeometryTableSpec('building')
        building.set_columns(poly_cols[:-1])
        
        boundary=opg.GeometryTableSpec('boundary')
        boundary.set_columns([p for p in poly_cols if p.name in ('osm_id','part','quadtree','tile','boundary','admin_level','name','ref', 'minzoom','way_area','way')])
        return [point,line,polygon,highway,building,boundary]
    return [point,line,polygon]

def make_polypoint_view(curs, table_prfx):
    curs.execute("select * from %s_polygon limit 0" % table_prfx)
    cols=", ".join('"%s"' % c[0] for c in curs.description if not (c[0]=='way' or c[1]=='way_point'))
    
    return "create view %%ZZ%%_polypoint as select %s, way_point as way from %%ZZ%%_polygon" % cols
    
    

def has_mem(ct,k):
    for a,b,c,d in ct:
        if k==a:
            return True
    return False
def make_tag_cols(coltags):
    common = [("osm_id","bigint"),("tile","bigint"), ("quadtree","bigint")]

    post = []
    if has_mem(coltags,'*'):
        #post.append(('other_tags','jsonb'))
        post.append(('other_tags','hstore'))
    elif has_mem(coltags, 'XXX'):
        #post.append(('tags', 'jsonb'))
        post.append(('tags', 'hstore'))
    if has_mem(coltags, 'layer'):
        post.append(('layer','int'))
    if has_mem(coltags,'minzoom'):
        post.append(('minzoom','integer'))
    point = common+[(a,"text" ) for a,b,c,d in coltags if b and not a in ("*","minzoom",'XXX','layer')]+post+[("way","geometry(Point,3857)")]
    line = common+[(a,"text") for a,b,c,d in coltags if c and not a in ("*","minzoom",'XXX','layer')]+post+[('z_order','int'),('length','float'),("way","geometry(LineString,3857)")]
    poly = common[:1]+[('part','int')]+common[1:]+[(a,"text") for a,b,c,d in coltags if d and not a in ("*","minzoom",'XXX','layer')]+post+[('z_order','int'),('way_area','float'),("way","geometry(Polygon,3857)")]
    
    return point,line,poly
    
    
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
    
    

def create_tables(curs, table_prfx,coltags):
    for t in ('point','line','polygon','roads','boundary','building','highway'):
        curs.execute("drop table if exists "+table_prfx+t+" cascade" )
    
    
    for ct in coltags:
        create = prep_table_create(table_prfx, ct)
        print(create)
        curs.execute(create)
        x="alter table %s%s set (autovacuum_enabled=false)" % (table_prfx, ct.table_name)
        print(x)
        curs.execute(x)
    return
    
    




def create_indices(curs, table_prfx, extraindices=False, vacuum=False, planet=False):
    write_indices(curs,table_prfx,indices)
    
    
    
    if extraindices:
        write_indices(curs,table_prfx,extras)
        
            
        
    if planet:
        write_indices(curs,table_prfx,planetosm)
        
    
    if vacuum:
        write_indices(curs,table_prfx,vacuums)
        if extraindices:
            write_indices(curs,table_prfx,vacuums_extra)
            
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

indices = [
"""CREATE TABLE %ZZ%roads AS
    SELECT osm_id,null as part,tile,quadtree,name,ref,admin_level,highway,railway,boundary,
            service,tunnel,bridge,z_order,covered,surface, minzoom, way
        FROM %ZZ%line
        WHERE highway in (
            'secondary','secondary_link','primary','primary_link',
            'trunk','trunk_link','motorway','motorway_link')
        OR railway is not null

    UNION SELECT osm_id,part,tile,quadtree,name,null as ref, admin_level,null as highway,
            null as railway, boundary, null as service,
            null as tunnel,null as bridge, 0  as z_order,null as covered,null as surface,minzoom, 
            st_exteriorring(way) as way
        FROM %ZZ%polygon WHERE
            osm_id<0 and boundary='administrative'""",
"CREATE INDEX %ZZ%point_way         ON %ZZ%point    USING gist  (way)",
"CREATE INDEX %ZZ%line_way          ON %ZZ%line     USING gist  (way)",
"CREATE INDEX %ZZ%polygon_way       ON %ZZ%polygon  USING gist  (way)",
"CREATE INDEX %ZZ%roads_way         ON %ZZ%roads    USING gist  (way)",]

vacuums = [
"vacuum analyze %ZZ%point",
"vacuum analyze %ZZ%line",
"vacuum analyze %ZZ%polygon",
"vacuum analyze %ZZ%roads",]

extras = [
"CREATE INDEX %ZZ%point_osmid       ON %ZZ%point    USING btree (osm_id)",
"CREATE INDEX %ZZ%line_osmid        ON %ZZ%line     USING btree (osm_id)",
"CREATE INDEX %ZZ%polygon_osmid     ON %ZZ%polygon  USING btree (osm_id)",
"CREATE INDEX %ZZ%roads_osmid       ON %ZZ%roads    USING btree (osm_id)",
"CREATE INDEX %ZZ%roads_admin       ON %ZZ%roads    USING gist  (way) WHERE boundary = 'administrative'",
"CREATE INDEX %ZZ%roads_roads_ref   ON %ZZ%roads    USING gist  (way) WHERE highway IS NOT NULL AND ref IS NOT NULL",
"CREATE INDEX %ZZ%roads_admin_low   ON %ZZ%roads    USING gist  (way) WHERE boundary = 'administrative' AND admin_level IN ('0', '1', '2', '3', '4')",
"CREATE INDEX %ZZ%line_ferry        ON %ZZ%line     USING gist  (way) WHERE route = 'ferry'",
"CREATE INDEX %ZZ%line_river        ON %ZZ%line     USING gist  (way) WHERE waterway = 'river'",
"CREATE INDEX %ZZ%line_name         ON %ZZ%line     USING gist  (way) WHERE name IS NOT NULL",
"CREATE INDEX %ZZ%polygon_military  ON %ZZ%polygon  USING gist  (way) WHERE landuse = 'military'",
"CREATE INDEX %ZZ%polygon_nobuilding ON %ZZ%polygon USING gist  (way) WHERE building IS NULL",
"CREATE INDEX %ZZ%polygon_name      ON %ZZ%polygon  USING gist  (way) WHERE name IS NOT NULL",
"CREATE INDEX %ZZ%polygon_way_area_z6 ON %ZZ%polygon USING gist (way) WHERE way_area > 59750",
"CREATE INDEX %ZZ%point_place       ON %ZZ%point    USING gist  (way) WHERE place IS NOT NULL AND name IS NOT NULL",
"create view %ZZ%highway as (select * from %ZZ%line where z_order is not null and z_order!=0)",
"""create table %ZZ%boundary as (select osm_id,part,tile,quadtree,name,admin_level,boundary,minzoom, 
            st_exteriorring(way) as way from %ZZ%polygon where osm_id<0 and boundary='administrative')""",
"create view %ZZ%building as (select * from %ZZ%polygon where building is not null and building != 'no')",
"create index %ZZ%highway_view on %ZZ%line using gist (way) where z_order is not null and z_order!=0",
"create index %ZZ%boundary_view on %ZZ%boundary using gist (way)",
"create index %ZZ%building_view on %ZZ%polygon using gist (way) where building is not null and building != 'no'",


]
vacuums_extra = [
'vacuum analyze %ZZ%boundary',
]

planetosm = [
"drop view if exists planet_osm_point",
"drop view if exists planet_osm_line",
"drop view if exists planet_osm_polygon",
"drop table if exists planet_osm_roads",
"drop view if exists planet_osm_highway",
"drop view if exists planet_osm_building",
"drop view if exists planet_osm_boundary",
"create view planet_osm_point as (select * from %ZZ%point)",
"create view planet_osm_line as (select * from %ZZ%line union all select * from %ZZ%highway)",
"create view planet_osm_polygon as (select * from %ZZ%polygon union all select * from %ZZ%building)",
"""create table planet_osm_roads as (
    SELECT osm_id,null as part,tile,quadtree,name,ref,admin_level,highway,railway,boundary,
            service,tunnel,bridge,z_order,covered,surface, minzoom, way
        FROM %ZZ%highway
        WHERE highway in (
            'secondary','secondary_link','primary','primary_link',
            'trunk','trunk_link','motorway','motorway_link')
        OR railway is not null

    UNION ALL
    
    SELECT osm_id,part,tile,quadtree,name,null as ref, admin_level,null as highway,
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


]

extended_indices = [
"create index %ZZ%point_way on %ZZ%point using gist(way)",
"create index %ZZ%line_way on %ZZ%line using gist(way)",
"create index %ZZ%polygon_way on %ZZ%polygon using gist(way)",
"create index %ZZ%highway_way on %ZZ%highway using gist(way)",
"create index %ZZ%building_way on %ZZ%building using gist(way)",
"create index %ZZ%boundary_way on %ZZ%boundary using gist(way)",
"""create index %ZZ%highway_way_lz on %ZZ%highway using gist(way) where (
    highway in ('motorway','motorway_link','trunk','trunk_link','primary','primary_link','secondary')
    or (railway in ('rail','light_rail','narrow_gauge','funicular') and (service IS NULL OR service NOT IN ('spur', 'siding', 'yard')))
)""",
"create index %ZZ%polygon_way_point on %ZZ%polygon using gist(way_point)",
"create index %ZZ%point_id on %ZZ%point using btree(osm_id)",
"create index %ZZ%line_id on %ZZ%line using btree(osm_id)",
"create index %ZZ%polygon_id on %ZZ%polygon using btree(osm_id)",
"create index %ZZ%highway_id on %ZZ%highway using btree(osm_id)",
"create index %ZZ%building_id on %ZZ%building using btree(osm_id)",
"create index %ZZ%boundary_id on %ZZ%boundary using btree(osm_id)",

"alter table %ZZ%point set (autovacuum_enabled=true)",
"alter table %ZZ%line set (autovacuum_enabled=true)",
"alter table %ZZ%polygon set (autovacuum_enabled=true)",
"alter table %ZZ%highway set (autovacuum_enabled=true)",
"alter table %ZZ%building set (autovacuum_enabled=true)",
"alter table %ZZ%boundary set (autovacuum_enabled=true)",
"vacuum analyze %ZZ%point",
"vacuum analyze %ZZ%line",
"vacuum analyze %ZZ%polygon",
"vacuum analyze %ZZ%highway",
"vacuum analyze %ZZ%building",
"vacuum analyze %ZZ%boundary",


]



def create_tables_lowzoom(curs, prfx, newprefix, minzoom, simp=None, cols=None, extended=False):
    
    
    queries = []
    
    table_names = ["point", "line", "polygon","roads"]
    if extended:
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
             
    
    if extended:
        for tab in table_names:
            queries.append("create index %ZZ%" + tab +"_way on %ZZ%" + tab +" using gist(way)")
            queries.append("create index %ZZ%" + tab +"_id on %ZZ%" + tab +" using btree(osm_id)")
        for tab in table_names:
            queries.append("vacuum analyze %ZZ%" + tab)
        
    else:
        queries += indices[1:]
        queries += extras
        queries += vacuums
        queries += vacuums_extra
    
    print("call %d queries..." % len(queries))
    write_indices(curs,newprefix,queries)
    
    
def create_views_lowzoom(curs, prfx, newprefix, minzoom, indicies=True,extended=False):
    
    
    
    queries=[]
    
    if extended:
        for tab in ("point", "line", "polygon","highway", "building", "boundary"):
            queries.append("drop view if exists %ZZ%"+tab)
            queries.append("create view %ZZ%"+tab+" as (select * from "+prfx+tab+" where minzoom <= "+str(minzoom)+")")
            if indices:
                queries.append("create index %ZZ%"+tab+"_way on "+prfx+tab+" using gist(way) where minzoom <= "+str(minzoom))
                
        
        
    else:
    
        for tab in ("point", "line", "polygon","roads", "highway", "building", "boundary"):
            queries.append("drop view if exists %ZZ%"+tab)
            queries.append("create view %ZZ%"+tab+" as (select * from "+prfx+tab+" where minzoom <= "+str(minzoom)+")")
        
        if indicies:
            for tab in ("point", "line", "polygon","roads","boundary"):
                queries.append("create index %ZZ%"+tab+"_way on "+prfx+tab+" using gist(way) where minzoom <= "+str(minzoom))
            
            queries.append("create index %ZZ%highway_way on "+prfx+"line using gist (way) where z_order is not null and z_order!=0 and minzoom <= "+str(minzoom))
            queries.append("create index %ZZ%building_view on "+prfx+"polygon using gist (way) where building is not null and building != 'no' and minzoom <= "+str(minzoom))
    write_indices(curs,newprefix,queries)
    

def write_to_postgis(prfx, box_in,connstr, tabprfx, stylefn=None, writeindices=True, lastdate=None,minzoom=None,nothread=False, numchan=4, minlen=0,minarea=5,extraindices=False,use_binary=True, extended=False):
    if not connstr or not tabprfx:
        raise Exception("must specify connstr and tabprfx")
        
        
    params,style = process.prep_geometry_params(prfx, box_in, stylefn, lastdate, minzoom, numchan, minlen, minarea)
    
    
    
    postgisparams = opg.PostgisParameters()
    
    #params.coltags = sorted((k,v.IsNode,v.IsWay,v.IsWay) for k,v in params.style.items() if k not in ('z_order','way_area'))
    postgisparams.coltags = postgis_columns(style, params.findmz is not None, extended)
    if extended:
        postgisparams.alloc_func='extended'
        postgisparams.validate_geometry = True
    
    if tabprfx and not tabprfx.endswith('_'):
        tabprfx = tabprfx+'_'
    postgisparams.tableprfx = tabprfx + ('_' if tabprfx and not tabprfx.endswith('_') else '')
    postgisparams.connstring = connstr
    postgisparams.use_binary=use_binary
    
    conn=None
    if postgisparams.connstring!='null':
        conn=psycopg2.connect(postgisparams.connstring)
        conn.autocommit=True
        create_tables(conn.cursor(), postgisparams.tableprfx, postgisparams.coltags)
    
        
    cnt,errs=None,None
    if nothread:
        errs = opg.process_geometry_postgis_nothread(params, postgisparams, Prog(locs=params.locs))
    else:
        errs = opg.process_geometry_postgis(params, postgisparams, None)#Prog(locs=params.locs))
        
    if writeindices and postgisparams.connstring!='null':
        if extended:
            write_indices(conn.cursor(),postgisparams.tableprfx, extended_indices)
        else:
            create_indices(conn.cursor(), postgisparams.tableprfx, extraindices, extraindices)
            write_indices(conn.cursor(), postgisparams.tableprfx, [make_polypoint_view(conn.cursor(), postgisparams.tableprfx)])

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
            
    
def write_to_csvfile(prfx, box_in,outfnprfx,  stylefn=None, lastdate=None,minzoom=None,nothread=False, numchan=4, minlen=0,minarea=5, use_binary=True,extended=False):
    
    params,style = process.prep_geometry_params(prfx, box_in, stylefn, lastdate, minzoom, numchan, minlen, minarea)
    
    
    #params.coltags = sorted((k,v.IsNode,v.IsWay,v.IsWay) for k,v in params.style.items() if k not in ('z_order','way_area'))
    
    postgisparams=opg.PostgisParameters()
    postgisparams.coltags = postgis_columns(style, params.findmz is not None, extended)
    if extended:
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
