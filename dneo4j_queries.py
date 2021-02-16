from neo4j import GraphDatabase
import os
import csv
import datetime
from neo4j.time import DateTime
import sys
import time
import cProfile, pstats
import io
import random
import string

class DNeo4j_Queries:
    
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.attrmap = {}
        self.init_attrmap()
        self.fkmap = {}
        self.init_fkmap()
        self.tableLst = []
        for key in self.attrmap:
            if key[:2] == 'L_':
                continue
            self.tableLst.append(key)
        self.linkmap = {}
        self.init_linkmap()
        self.link2rel = {}
        for key in self.attrmap:
            if key[:2] == 'L_':
                self.link2rel[key] = []
        self.table2rel = {}
        for key in self.attrmap:
            if key[:2] != 'L_':
                self.table2rel[key] = []
        self.init_linknames()
        self.init_relnames()
    
    def init_attrmap(self):
        self.attrmap["H_UserType"] = ['id', 'version', 'timestamp',
                                      'user_id']
        self.attrmap["H_User"] = ['id', 'name', 'version', 'timestamp',
                                  'user_id']
        self.attrmap["H_AssetType"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["H_Asset"] = ['id', 'name', 'version', 'timestamp',
                                   'user_id']
        self.attrmap["H_WhoProfile"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["H_WhatProfile"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["H_HowProfile"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["H_WhyProfile"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["H_WhenProfile"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["H_SourceType"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["H_Source"] = ['id', 'name', 'version', 'timestamp',
                                   'user_id']
        self.attrmap["H_WhereProfile"] = ['id', 'access_path', 'version',
                                          'timestamp', 'user_id']
        self.attrmap["H_Action"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["H_RelationshipType"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["H_Relationship"] = ['id', 'version', 'timestamp',
                                       'user_id']
        self.attrmap["L_UserTypeLink"] = ['id', 'user_id', 'user_type_id',
                                          'version', 'timestamp', 'write_user_id']
        self.attrmap["L_AssetTypeLink"] = ['id', 'asset_id', 'asset_type_id',
                                           'version', 'timestamp', 'user_id']
        self.attrmap["L_Asset_WhoProfile"] = ['id', 'asset_id', 'who_profile_id',
                                              'version', 'timestamp', 'user_id']
        self.attrmap["L_WhoProfileUser"] = ['id', 'who_profile_id', 'who_user_id',
                                            'version', 'timestamp', 'user_id']
        self.attrmap["L_Asset_HowProfile"] = ['id', 'asset_id', 'how_profile_id',
                                              'version', 'timestamp', 'user_id']
        self.attrmap["L_Asset_WhyProfile"] = ['id', 'asset_id', 'why_profile_id',
                                              'version', 'timestamp', 'user_id']
        self.attrmap["L_Asset_WhatProfile"] = ['id', 'asset_id', 'what_profile_id',
                                               'version', 'timestamp', 'user_id']
        self.attrmap["L_Asset_WhenProfile"] = ['id', 'asset_id', 'when_profile_id',
                                               'version', 'timestamp', 'user_id']
        self.attrmap["L_Source2Type"] = ['id', 'source_id', 'source_type_id',
                                         'version', 'timestamp', 'user_id']
        self.attrmap["L_Asset_WhereProfile"] = ['id', 'asset_id', 'where_profile_id',
                                                'version', 'timestamp', 'user_id']
        self.attrmap["L_AssetsInActions"] = ['id', 'action_id', 'asset_id', 'who_profile_id',
                                             'why_profile_id', 'when_profile_id', 'how_profile_id',
                                             'version', 'timestamp', 'user_id']
        self.attrmap["L_Relationship_Type"] = ['id', 'relationship_id', 'relationship_type_id',
                                               'version', 'timestamp', 'user_id']
        self.attrmap["L_Asset_Relationships"] = ['id', 'asset_id', 'relationship_id',
                                                 'version', 'timestamp', 'user_id']
        self.attrmap["S_User_schema"] = ['id', 'user_id', 'schema', 
                                         'version', 'timestamp', 'write_user_id']
        self.attrmap["S_WhoProfile_schema"] = ['id', 'who_profile_id', 'schema',
                                               'user_id', 'version', 'timestamp',
                                               'write_user_id']
        self.attrmap["S_HowProfile_schema"] = ['id', 'how_profile_id', 'schema',
                                               'version', 'timestamp', 'user_id']
        self.attrmap["S_WhyProfile_schema"] = ['id', 'why_profile_id', 'schema',
                                               'version', 'timestamp', 'user_id']
        self.attrmap["S_WhatProfile_schema"] = ['id', 'what_profile_id', 'schema',
                                                'version', 'timestamp', 'user_id']
        self.attrmap["S_WhenProfile_Attributes"] = ['id', 'h_when_id', 'asset_timestamp',
                                                    'expiry_date', 'start_date', 'version',
                                                    'timestamp', 'user_id']
        self.attrmap["S_Configuration"] = ['id', 'schema', 'where_profile_id',
                                           'version', 'timestamp', 'user_id']
        self.attrmap["S_SourceTypeAttributes"] = ['id', 'source_type_id', 'connector',
                                                  'serdetype', 'datamodel', 'version',
                                                  'timestamp', 'user_id']
        self.attrmap["S_AssetTypeAttributes"] = ['id', 'name', 'description', 'version',
                                                 'timestamp', 'user_id']
        self.attrmap["S_UserTypeAttributes"] = ['id', 'name', 'description',
                                                'version', 'timestamp', 'user_id']
        self.attrmap["S_RelationshipTypeAttributes"] = ['id', 'name', 'description',
                                                        'version', 'timestamp', 'user_id']
        self.attrmap["S_Relationship_schema"] = ['id', 'relationship_id', 'schema',
                                                 'version', 'timestamp', 'user_id']
        self.attrmap["S_Source_schema"] = ['id', 'schema', 'version', 'timestamp', 'user_id']
        self.attrmap["L_WhereProfile_Source"] = ['id', 'source_id', 'where_profile_id',
                                                 'version', 'timestamp', 'user_id']
        
    
    def init_fkmap(self):
        self.fkmap['H_User'] = [('user_id', 'H_User')]
        self.fkmap['H_UserType'] = [('user_id', 'H_User')]
        self.fkmap['H_AssetType'] = [('user_id', 'H_User')]
        self.fkmap['H_Asset'] = [('user_id', 'H_User')]
        self.fkmap['H_WhoProfile'] = [('user_id', 'H_User')]
        self.fkmap['H_WhatProfile'] = [('user_id', 'H_User')]
        self.fkmap['H_HowProfile'] = [('user_id', 'H_User')]
        self.fkmap['H_WhyProfile'] = [('user_id', 'H_User')]
        self.fkmap['H_WhenProfile'] = [('user_id', 'H_User')]
        self.fkmap['H_SourceType'] = [('user_id', 'H_User')]
        self.fkmap['H_Source'] = [('user_id', 'H_User')]
        self.fkmap['H_WhereProfile'] = [('user_id', 'H_User')]
        self.fkmap['H_Action'] = [('user_id', 'H_User')]
        self.fkmap['H_RelationshipType'] = [('user_id', 'H_User')]
        self.fkmap['H_Relationship'] = [('user_id', 'H_User')]
        self.fkmap['L_UserTypeLink'] = [('user_id', 'H_User'), ('user_type_id', 'H_UserType'),
                                        ('write_user_id', 'H_User')]
        self.fkmap['L_AssetTypeLink'] = [('user_id', 'H_User'), ('asset_id', 'H_Asset'),
                                         ('asset_type_id', 'H_AssetType')]
        self.fkmap['L_Asset_WhoProfile'] = [('user_id', 'H_User'), ('who_profile_id', 'H_WhoProfile'),
                                            ('asset_id', 'H_Asset')]
        self.fkmap['L_WhoProfileUser'] = [('user_id', 'H_User'), ('who_profile_id', 'H_WhoProfile'),
                                          ('who_user_id', 'H_User')]
        self.fkmap['L_Asset_HowProfile'] = [('user_id', 'H_User'), ('how_profile_id', 'H_HowProfile'),
                                            ('asset_id', 'H_Asset')]
        self.fkmap['L_Asset_WhyProfile'] = [('user_id', 'H_User'), ('asset_id', 'H_Asset'),
                                            ('why_profile_id', 'H_WhyProfile')]
        self.fkmap['L_Asset_WhatProfile'] = [('user_id', 'H_User'), ('asset_id', 'H_Asset'),
                                            ('what_profile_id', 'H_WhatProfile')]
        self.fkmap['L_Asset_WhenProfile'] = [('user_id', 'H_User'), ('asset_id', 'H_Asset'),
                                            ('when_profile_id', 'H_WhenProfile')]
        self.fkmap['L_Source2Type'] = [('source_id', 'H_Source'), ('source_type_id', 'H_SourceType'),
                                       ('user_id', 'H_User')]
        self.fkmap['L_Asset_WhereProfile'] = [('user_id', 'H_User'), ('asset_id', 'H_Asset'),
                                              ('where_profile_id', 'H_WhereProfile')]
        self.fkmap['L_AssetsInActions'] = [('action_id', 'H_Action'), ('asset_id', 'H_Asset'),
                                           ('who_profile_id', 'H_WhoProfile'),
                                           ('why_profile_id', 'H_WhyProfile'),
                                           ('when_profile_id', 'H_WhenProfile'),
                                           ('how_profile_id', 'H_HowProfile'),
                                           ('user_id', 'H_User')]
        self.fkmap['L_Relationship_Type'] = [('relationship_id', 'H_Relationship'),
                                             ('relationship_type_id', 'H_RelationshipType'),
                                             ('user_id', 'H_User')]
        self.fkmap['L_Asset_Relationships'] = [('relationship_id', 'H_Relationship'),
                                               ('asset_id', 'H_Asset'), ('user_id', 'H_User')]
        self.fkmap['S_User_schema'] = [('user_id', 'H_User'), ('write_user_id', 'H_User')]
        self.fkmap['S_WhoProfile_schema'] = [('user_id', 'H_User'), ('who_profile_id', 'H_WhoProfile'),
                                             ('write_user_id', 'H_User')]
        self.fkmap['S_HowProfile_schema'] = [('user_id', 'H_User'), ('how_profile_id', 'H_HowProfile')]
        self.fkmap['S_WhyProfile_schema'] = [('user_id', 'H_User'), ('why_profile_id', 'H_WhyProfile')]
        self.fkmap['S_WhatProfile_schema'] = [('user_id', 'H_User'), ('what_profile_id', 'H_WhatProfile')]
        self.fkmap['S_WhenProfile_Attributes'] = [('user_id', 'H_User'), ('h_when_id', 'H_WhenProfile'),
                                                  ]
        self.fkmap['S_Configuration'] = [('user_id', 'H_User'), ('where_profile_id', 'H_WhereProfile')]
        self.fkmap['S_SourceTypeAttributes'] = [('source_type_id', 'H_SourceType'),
                                                ('user_id', 'H_User')]
        self.fkmap['S_AssetTypeAttributes'] = [('user_id', 'H_User')]
        self.fkmap['S_UserTypeAttributes'] = [('user_id', 'H_User')]
        self.fkmap['S_RelationshipTypeAttributes'] = [('user_id', 'H_User')]
        self.fkmap['S_Relationship_schema'] = [('user_id', 'H_User'),
                                               ('relationship_id', 'H_Relationship')]
        self.fkmap['S_Source_schema'] = [('user_id', 'H_User')]
        self.fkmap['L_WhereProfile_Source'] = [('source_id', 'H_Source'), ('where_profile_id', 'H_WhereProfile'),
                                               ('user_id', 'H_User')]
    
    def init_linkmap(self):
        self.linkmap['L_UserTypeLink'] = [('user_id', 'H_User'), ('user_type_id', 'H_UserType')]
        self.linkmap['L_AssetTypeLink'] = [('asset_id', 'H_Asset'), ('asset_type_id', 'H_AssetType')]
        self.linkmap['L_Asset_WhoProfile'] = [('who_profile_id', 'H_WhoProfile'), ('asset_id', 'H_Asset')]
        self.linkmap['L_WhoProfileUser'] = [('who_profile_id', 'H_WhoProfile'), ('who_user_id', 'H_User')]
        self.linkmap['L_Asset_HowProfile'] = [('how_profile_id', 'H_HowProfile'),
                                            ('asset_id', 'H_Asset')]
        self.linkmap['L_Asset_WhyProfile'] = [('why_profile_id', 'H_WhyProfile'), ('asset_id', 'H_Asset')]
        self.linkmap['L_Asset_WhatProfile'] = [('what_profile_id', 'H_WhatProfile'), ('asset_id', 'H_Asset')]
        self.linkmap['L_Asset_WhenProfile'] = [('when_profile_id', 'H_WhenProfile'), ('asset_id', 'H_Asset')]
        self.linkmap['L_Source2Type'] = [('source_id', 'H_Source'), ('source_type_id', 'H_SourceType')]
        self.linkmap['L_Asset_WhereProfile'] = [('where_profile_id', 'H_WhereProfile'),
                                                ('asset_id', 'H_Asset')]
        self.linkmap['L_AssetsInActions'] = [[('action_id', 'H_Action'), ('asset_id', 'H_Asset')],
                                             [('action_id', 'H_Action'), ('who_profile_id', 'H_WhoProfile')],
                                             [('action_id', 'H_Action'), ('why_profile_id', 'H_WhyProfile')],
                                             [('action_id', 'H_Action'), ('when_profile_id', 'H_WhenProfile')],
                                             [('action_id', 'H_Action'), ('how_profile_id', 'H_HowProfile')]]
        self.linkmap['L_Relationship_Type'] = [('relationship_id', 'H_Relationship'), ('relationship_type_id', 'H_RelationshipType')]
        self.linkmap['L_Asset_Relationships'] = [('relationship_id', 'H_Relationship'),
                                                 ('asset_id', 'H_Asset')]
        self.linkmap['L_WhereProfile_Source'] = [('where_profile_id', 'H_WhereProfile'), ('source_id', 'H_Source')]
    
    def init_linknames(self):
        for key in self.linkmap:
            if key == 'L_AssetsInActions':
                for e in self.linkmap[key]:
                    st_node = e[0]
                    end_node = e[1]
                    st_fkname = st_node[1]
                    end_fkname = end_node[1]
                    rel_name = 'Rel_' + st_fkname + '_' + end_fkname
                    self.link2rel[key].append(rel_name)
            else:
                st_node = self.linkmap[key][0]
                end_node = self.linkmap[key][1]
                st_fkname = st_node[1]
                end_fkname = end_node[1]
                #rel_name = 'Rel_' + st_fkname + '_' + end_fkname
                #we need names that are guaranteed to be unique!
                #...even if that means the names end up being really long...
                rel_name = 'Rel_' + key + '_' + st_fkname + '_' + end_fkname
                self.link2rel[key].append(rel_name)
    
    def init_relnames(self):
        for key in self.fkmap:
            #create_link_headers already handles links
            if key[:2] == 'L_':
                continue
            print("Handling key: " + str(key))
            for p in self.fkmap[key]:
                fk = p[0]
                fk_name = p[1]
                rel_name = 'Rel_' + key + '_' + fk_name
                if fk == 'write_user_id':
                    rel_name = 'Rel_' + key + '_Write_' + fk_name 
                
                self.table2rel[key].append(rel_name)
    
    def getDataType(self, attrName):
        if 'id' in attrName:
            return 'int'
        elif attrName == 'version':
            return 'int'
        elif 'timestamp' in attrName or 'date' in attrName:
            return 'datetime'
        else:
            return 'string'
    
    def create_indexes(self):
        for tname in self.tableLst:
            query_str = "CREATE CONSTRAINT ON (p:" + tname + ") "
            query_str += "ASSERT p.id IS UNIQUE"
            with self.driver.session() as session:
                session.run(query_str)
    
    def get_lastId(self, tname):
        query_str = "MATCH (n:" + tname + ") RETURN max(n.id) AS max_id;"
        print(query_str)
        with self.driver.session() as session:
            result = session.run(query_str)
            record = result.single()
            print(tname + ": " + str(record))
            print("Max of " + tname + " is: " + str(record["max_id"]))
            return record["max_id"]
    
    def make_q1_queries(self, tname):
        query_strs = []
        qp1 = "UNWIND $props AS map CREATE (n:H_WhatProfile) SET n = map"
        qp1 += " RETURN n"
        qp2 = "MATCH (m:H_User) WHERE m.id = n.user_id CREATE (n)-[:Rel_H_WhatProfile_H_User]->(m)"
        
        q1 = 'call apoc.periodic.iterate("' + qp1 + '", '
        q1 += '"' + qp2 + '", {batchSize:1000}) YIELD batches, total, errorMessages return batches, total, errorMessages'
        
        query_strs.append(q1)
        
        qp3 = "UNWIND $props AS map CREATE (n:S_WhatProfile_schema) SET n = map"
        qp3 += " RETURN n"
        qp4 = "MATCH (m:H_User) WHERE m.id = n.user_id CREATE (n)-[:Rel_S_WhatProfile_schema_H_User]->(m) "
        qp4 += "MATCH (m2:H_WhatProfile) WHERE m2.id = n.what_profile_id CREATE (n)-[:Rel_S_WhatProfile_schema_H_WhatProfile]->(m2) "
        qp4 += "MATCH (m3:"
        q2 = 'call apoc.periodic.iterate("' + qp3 + '", '
        q2 += '"' + qp4 + '", {batchSize:1000}) YIELD batches, total, errorMessages return batches, total, errorMessages'
        
        query_strs.append(q2)
        
        return query_strs
    
    def get_q1inserts(self, tname, tid, qnum, x):
        recmap = []
        recs = []
        numrows = 0
        with open(tname + '.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in csvreader:
                if numrows >= x:
                    break
                numrows += 1
                recs.append(row)
        
        attrs = self.attrmap[tname]
        for i,r in enumerate(recs):
            tmpdict = {}
            for j,a in enumerate(attrs):
                dtype = self.getDataType(a)
                if dtype == 'int' and 'asset_id' in a:
                    tmpdict[a] = r[j]
                elif dtype == 'int' and a == 'id':
                    tmpdict[a] = tid + i + 1
                elif dtype == 'int':
                    tmpdict[a] = int(r[j])
                elif dtype == 'datetime':
                    dt = datetime.datetime.strptime(r[j], '%Y-%m-%dT%H:%M:%S')
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    tmpdict[a] = nd
                else:
                    tmpdict[a] = r[j]
            recmap.append(tmpdict)
        
        return recmap
    
    def get_q1links(self, tname, tid, hid, qnum, x):
        recmap = []
        recs = []
        numrows = 0
        with open(tname + '.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in csvreader:
                if numrows >= x:
                    break
                numrows += 1
                recs.append(row)
        
        attrs = self.attrmap[tname]
        for i,r in enumerate(recs):
            tmpdict = {}
            for j,a in enumerate(attrs):
                dtype = self.getDataType(a)
                if dtype == 'int' and 'profile' in a:
                    tmpdict[a] = hid + i + 1
                elif dtype == 'int' and a == 'id':
                    tmpdict[a] = tid + i + 1
                elif dtype == 'int':
                    tmpdict[a] = int(r[j])
                elif dtype == 'datetime':
                    dt = datetime.datetime.strptime(r[j], '%Y-%m-%dT%H:%M:%S')
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    tmpdict[a] = nd
                else:
                    tmpdict[a] = r[j]
            recmap.append(tmpdict)
        
        return recmap
        
    
    def execute_q1(self):
        hname = 'H_WhatProfile'
        sname = 'S_WhatProfile_schema'
        lname = 'L_Asset_WhatProfile'
        hdname = '/home/pranav/catalog_experiments/' + hname
        sdname = '/home/pranav/catalog_experiments/' + sname
        ldname = '/home/pranav/catalog_experiments/' + lname
        h_id = self.get_lastId(hname)
        s_id = self.get_lastId(sname)
        l_id = h_id #we don't actually need this to be unique
        h_what = self.get_q1inserts(hname, h_id, 1, 100000)
        s_what = self.get_q1links(sname, s_id, h_id, 1, 100000)
        l_what = self.get_q1links(lname, l_id, h_id, 1, 100000)
        
        h_query = 'UNWIND $props AS map CREATE (n:' + hname + ') SET n = map RETURN n.id AS nid'
        h_relqueries = []
        for p in self.fkmap[hname]:
            fk = p[0]
            fk_name = p[1]
            rel2use = ''
            for rel_name in self.table2rel[hname]:
                print("rel_name: " + rel_name)
                if hname in rel_name and fk_name in rel_name:
                    rel2use = rel_name
            
            if rel2use == '':
                print("ERROR: rel2use was empty!")
                print("fkname: " + fk_name)
                print("hname: " + hname)
            
            relquery = 'UNWIND $nids as nid MATCH (n:' + hname + ' {id: nid}) '
            relquery += ' MATCH (m:' + fk_name + ') WHERE m.id = n.' + fk
            relquery += ' CREATE (n)-[:' + rel2use + ']->(m)'
            h_relqueries.append(relquery)
        
        #now, add link relationships
        st_node = self.linkmap[lname][0]
        end_node = self.linkmap[lname][1]
        st_fk = st_node[0]
        st_fkname = st_node[1]
        end_fk = end_node[0]
        end_fkname = end_node[1]
        if st_fkname != hname:
            print("Q1: Using Wrong Link:")
            print("st_fkname: " + st_fkname)
            print("hname: " + hname)
        
        link2use = ''
        for rel_name in self.link2rel[lname]:
            if lname in rel_name and st_fkname in rel_name and end_fkname in rel_name:
                link2use = rel_name
        
        if link2use == '':
            print("ERROR: link2use was empty!")
            print("st_fkname: " + st_fkname)
            print("end_fkname: " + end_fkname)
            print("lname: " + lname)
        
        linkquery = 'UNWIND $props as map MATCH (n:' + st_fkname + ' {id: map.' + st_fk + '}) '
        linkquery += ' MATCH (m:' + end_fkname + ') WHERE m.id = map.' + end_fk
        linkquery += ' CREATE (n)-[r:' + link2use + ']->(m) SET r = map'
        
        s_relqueries = []
        s_query = 'UNWIND $props AS map CREATE (n:' + sname + ') SET n = map RETURN n.id AS nid'
        for p in self.fkmap[sname]:
            fk = p[0]
            fk_name = p[1]
            rel2use = ''
            for rel_name in self.table2rel[sname]:
                if sname in rel_name and fk_name in rel_name:
                    rel2use = rel_name
            
            if rel2use == '':
                print("ERROR: s-rel2use was empty!")
                print("fkname: " + fk_name)
                print("sname: " + sname)
            
            relquery = 'UNWIND $nids as nid MATCH (n:' + sname + ' {id: nid}) '
            relquery += ' MATCH (m:' + fk_name + ') WHERE m.id = n.' + fk
            relquery += ' CREATE (n)-[:' + rel2use + ']->(m)'
            s_relqueries.append(relquery)
            
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            res1 = session.run(h_query, props=h_what)
            hnewIds = [record["nid"] for record in res1]
            for r in h_relqueries:
                session.run(r, nids=hnewIds)
            
            res2 = session.run(s_query, props=s_what)
            snewIds = [record["nid"] for record in res2]
            for r in s_relqueries:
                session.run(r, nids=snewIds)
            
            session.run(linkquery, props=l_what)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q1_dneo4j_test.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q2(self):
        #adds howprofiles to the last asset
        hname = 'H_HowProfile'
        lname = 'L_Asset_HowProfile'
        sname = 'S_HowProfile_schema'
        h_asset_id = self.get_lastId('H_Asset')
        h_profid = self.get_lastId('H_HowProfile') + 1
        #l_profid = self.get_lastId('L_Asset_HowProfile') + 1
        #again, link ID doesn't matter here
        l_profid = h_profid
        s_profid = self.get_lastId('S_HowProfile_schema') + 1
        
        schema = '\'{ denoisingProcedure : run denoise.py with normalize set to true }\' '
        version = 1
        uid = 1
        dt = datetime.datetime.now()
        nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
        h_query = 'MERGE (n:H_HowProfile {id: ' + str(h_profid) + ','
        h_query += ' version: ' + str(version) + ', ' + 'timestamp: $nd ,'
        h_query += ' user_id: ' + str(uid) + '})'
        h_relqueries = []
        for p in self.fkmap[hname]:
            fk = p[0]
            fk_name = p[1]
            rel2use = ''
            for rel_name in self.table2rel[hname]:
                if hname in rel_name and fk_name in rel_name:
                    rel2use = rel_name
            
            if rel2use == '':
                print("Q2 ERROR: rel2use was empty!")
                print("fkname: " + fk_name)
                print("hname: " + hname)
            
            relquery = 'MATCH (n:' + hname + ' {id: ' + str(h_profid) + '}) '
            relquery += ' MATCH (m:' + fk_name + ') WHERE m.id = n.' + fk
            relquery += ' CREATE (n)-[:' + rel2use + ']->(m)'
            h_relqueries.append(relquery)
        
        s_query = 'MERGE (n:S_HowProfile_schema {id: ' + str(s_profid) + ','
        s_query += ' how_profile_id: ' + str(h_profid) + ','
        s_query += ' schema: ' + str(schema) + ',' + ' version: ' + str(version)
        s_query += ', timestamp: $nd, user_id: ' + str(uid) + '})'
        s_relqueries = []
        for p in self.fkmap[sname]:
            fk = p[0]
            fk_name = p[1]
            rel2use = ''
            for rel_name in self.table2rel[sname]:
                if sname in rel_name and fk_name in rel_name:
                    rel2use = rel_name
            
            if rel2use == '':
                print("Q2 ERROR: rel2use was empty!")
                print("fkname: " + fk_name)
                print("hname: " + sname)
            
            relquery = 'MATCH (n:' + sname + ' {id: ' + str(s_profid) + '}) '
            relquery += ' MATCH (m:' + fk_name + ') WHERE m.id = n.' + fk
            relquery += ' CREATE (n)-[:' + rel2use + ']->(m)'
            s_relqueries.append(relquery)
        
        #links
        st_node = self.linkmap[lname][0]
        end_node = self.linkmap[lname][1]
        st_fkname = st_node[1]
        end_fkname = end_node[1]
        if st_fkname != hname:
            print('Q2 ERROR: st_fkname != hname:')
            print('hname: ' + hname)
            print('st_fkname: ' + st_fkname)
        link2use = ''
        for rel_name in self.link2rel[lname]:
            if lname in rel_name and st_fkname in rel_name and end_fkname in rel_name:
                link2use = rel_name
        
        if link2use == '':
            print("Q2 ERROR: link2use was empty!")
            print("st_fkname: " + st_fkname)
            print("end_fkname: " + end_fkname)
            print("lname: " + lname)
        
        l_query = 'MATCH (n:' + hname + '{id: ' + str(h_profid) + '}) '
        l_query += 'MATCH (m:' + end_fkname + ') WHERE m.id = ' + str(h_asset_id)
        l_query += ' MERGE (n)-[:' + link2use + ' {id: ' + str(l_profid) + ','
        l_query += ' asset_id: ' + str(h_asset_id) + ', how_profile_id: '
        l_query +=  str(h_profid) + ', version: ' + str(version) + ', timestamp: $nd,'
        l_query += ' user_id: ' + str(uid) + '}]->(m)'
        
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(h_query, nd=nd)
            for r in h_relqueries:
                session.run(r)
            
            session.run(s_query, nd=nd)
            for r in s_relqueries:
                session.run(r)
            
            session.run(l_query, nd=nd)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q2_dneo4j_test.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_sat_query(self, tname, inserts):
        query_str1 = "UNWIND $props AS map CREATE (n:" + tname + ") SET n = map"
        query_str1 += " RETURN n.id AS nid"
        trel_queries = []
        for p in self.fkmap[tname]:
                fk = p[0]
                fk_name = p[1]
                var_name = fk_name.lower()
                rel2use = ''
                for rel_name in self.table2rel[tname]:
                    if tname in rel_name and fk_name in rel_name:
                        rel2use = rel_name
                node_part = "MATCH (n:" + tname + "{id: nid}) "
                node_part += "MATCH (" + var_name + ":" + fk_name + ") WHERE "
                node_part += var_name + ".id = n." + fk + " "
                rel_part = "CREATE (n)-[:" + rel2use + "]->("
                rel_part += var_name + ")"
                rel_query = "UNWIND $nids AS nid " + node_part + rel_part
                trel_queries.append(rel_query)
        
        newIdrecs = None
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            newIdrecs = session.run(query_str1, props=inserts)
        
            newIds = [record["nid"] for record in newIdrecs]
            for r in trel_queries:
                session.run(r, nids=newIds)
            #print(newIds)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q3_dneo4j_' + tname + '_test.txt', 'w+') as f:
            f.write(s.getvalue())
    
    #This is intended only for hubs! We need a separate func for satellites
    #linkdict--key is tname, value is another dictionary
    #key is lname, value is list of dicts, where each dict is the inserts
    #for that link
    def execute_bulk_query(self, tname, inserts, linkdict):
        query_str1 = "UNWIND $props AS map CREATE (n:" + tname + ") SET n = map"
        query_str1 += " RETURN n.id AS nid"
        trel_queries = []
        for p in self.fkmap[tname]:
                fk = p[0]
                fk_name = p[1]
                var_name = fk_name.lower()
                rel2use = ''
                for rel_name in self.table2rel[tname]:
                    if tname in rel_name and fk_name in rel_name:
                        rel2use = rel_name
                node_part = "MATCH (n:" + tname + "{id: nid}) "
                node_part += "MATCH (" + var_name + ":" + fk_name + ") WHERE "
                node_part += var_name + ".id = n." + fk + " "
                rel_part = "CREATE (n)-[:" + rel2use + "]->("
                rel_part += var_name + ")"
                rel_query = "UNWIND $nids AS nid " + node_part + rel_part
                trel_queries.append(rel_query)
        
        tlink_queries = []
        for lname in linkdict[tname]:
            linserts = linkdict[tname][lname]
            if lname != 'L_AssetsInActions':
                st_node = self.linkmap[lname][0]
                end_node = self.linkmap[lname][1]
                st_fkname = st_node[1]
                st_fk = st_node[0]
                end_fkname = end_node[1]
                end_fk = end_node[0]
                if st_fkname != tname:
                    print('Q3 ERROR: st_fkname != tname:')
                    print('tname: ' + tname)
                    print('st_fkname: ' + st_fkname)
                link2use = ''
                for rel_name in self.link2rel[lname]:
                    if lname in rel_name and st_fkname in rel_name and end_fkname in rel_name:
                        link2use = rel_name
                
                if link2use == '':
                    print("Q3 ERROR: link2use was empty!")
                    print("st_fkname: " + st_fkname)
                    print("end_fkname: " + end_fkname)
                    print("lname: " + lname)
                
                linkquery = 'UNWIND $props as map MATCH (n:' + st_fkname + ' {id: map.' + st_fk + '}) '
                linkquery += ' MATCH (m:' + end_fkname + ') WHERE m.id = map.' + end_fk
                linkquery += ' CREATE (n)-[r:' + link2use + ']->(m) SET r = map'
                tlink_queries.append((linkquery, linserts[0]))
            else:
                for i,e in enumerate(self.linkmap[lname]):
                    st_node = e[0]
                    end_node = e[1]
                    st_fkname = st_node[1]
                    st_fk = st_node[0]
                    end_fkname = end_node[1]
                    end_fk = end_node[0]
                    if st_fkname != tname:
                        print('Q3 link ERROR: st_fkname != tname:')
                        print('tname: ' + tname)
                        print('st_fkname: ' + st_fkname)
                    link2use = ''
                    for rel_name in self.link2rel[lname]:
                        if st_fkname in rel_name and end_fkname in rel_name:
                            link2use = rel_name
                
                    if link2use == '':
                        print("Q3 ERROR: link2use was empty!")
                        print("st_fkname: " + st_fkname)
                        print("end_fkname: " + end_fkname)
                        print("lname: " + lname)
                
                    linkquery = 'UNWIND $props as map MATCH (n:' + st_fkname + ' {id: map.' + st_fk + '}) '
                    linkquery += ' MATCH (m:' + end_fkname + ') WHERE m.id = map.' + end_fk
                    linkquery += ' CREATE (n)-[r:' + link2use + ']->(m) SET r = map'
                    print("index: " + str(i))
                    tlink_queries.append((linkquery, linserts[i]))
                
        
        
        newIdrecs = None
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            newIdrecs = session.run(query_str1, props=inserts)
        
            newIds = [record["nid"] for record in newIdrecs]
            for r in trel_queries:
                session.run(r, nids=newIds)
            
            for l in tlink_queries:
                session.run(l[0], props=l[1])
            #print(newIds)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q3_dneo4j_' + tname + '_test.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def get_q3inserts(self, tname, tid, asset_id, qnum, x):
        recmap = []
        recs = []
        numrows = 0
        with open(tname + '.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in csvreader:
                if numrows >= x:
                    break
                numrows += 1
                recs.append(row)
        
        attrs = self.attrmap[tname]
        for i,r in enumerate(recs):
            tmpdict = {}
            for j,a in enumerate(attrs):
                dtype = self.getDataType(a)
                if dtype == 'int' and 'asset_id' in a:
                    tmpdict[a] = asset_id
                elif dtype == 'int' and a == 'id':
                    tmpdict[a] = tid + i + 1
                elif dtype == 'int':
                    tmpdict[a] = int(r[j])
                elif dtype == 'datetime':
                    dt = datetime.datetime.strptime(r[j], '%Y-%m-%dT%H:%M:%S')
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    tmpdict[a] = nd
                else:
                    tmpdict[a] = r[j]
            recmap.append(tmpdict)
        
        return recmap
    
    def get_q3links(self, tname, tid, hid, asset_id, qnum, x):
        recmap = []
        recs = []
        numrows = 0
        with open(tname + '.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in csvreader:
                if numrows >= x:
                    break
                numrows += 1
                recs.append(row)
        
        attrs = self.attrmap[tname]
        for i,r in enumerate(recs):
            tmpdict = {}
            for j,a in enumerate(attrs):
                dtype = self.getDataType(a)
                if dtype == 'int' and ('profile' in a or a == 'h_when_id'):
                    tmpdict[a] = hid + i + 1
                elif dtype == 'int' and 'asset_id' in a:
                    tmpdict[a] = asset_id
                elif dtype == 'int' and a == 'id':
                    tmpdict[a] = tid + i + 1
                elif dtype == 'int':
                    tmpdict[a] = int(r[j])
                elif dtype == 'datetime':
                    dt = datetime.datetime.strptime(r[j], '%Y-%m-%dT%H:%M:%S')
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    tmpdict[a] = nd
                else:
                    tmpdict[a] = r[j]
            recmap.append(tmpdict)
        
        return recmap
    
    def get_q3actions(self, tname, tid, hname, hid, asset_id, end_fk,end_id, qnum, x):
        recmap = []
        recs = []
        numrows = 0
        with open(tname + '.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in csvreader:
                if numrows >= x:
                    break
                numrows += 1
                recs.append(row)
        
        attrs = self.attrmap[tname]
        for i,r in enumerate(recs):
            tmpdict = {}
            for j,a in enumerate(attrs):
                dtype = self.getDataType(a)
                if dtype == 'int' and a == end_fk and end_fk == 'asset_id':
                    tmpdict[a] = asset_id
                if dtype == 'int' and a == end_fk:
                    tmpdict[a] = end_id + i + 1
                elif dtype == 'int' and a == 'id':
                    tmpdict[a] = tid + i + 1
                elif dtype == 'int' and a == hname:
                    tmpdict[a] = hid + i + 1
                elif dtype == 'int' and (a == 'version'or a == 'user_id'):
                    tmpdict[a] = int(r[j])
                elif dtype == 'datetime':
                    dt = datetime.datetime.strptime(r[j], '%Y-%m-%dT%H:%M:%S')
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    tmpdict[a] = nd
                elif dtype == 'int':
                    tmpdict[a] = int(r[j])
                else:
                    tmpdict[a] = r[j]
            
            recmap.append(tmpdict)
        
        return recmap
                    
    
    def execute_q3(self, x):
        common_asset = self.get_lastId('H_Asset') + 1
        adict = {}
        with open('H_Asset.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            firstrow = []
            for i,row in enumerate(csvreader):
                if i > 0:
                    break
                else:
                    firstrow = row
            
            
            for i,a in enumerate(self.attrmap['H_Asset']):
                dtype = self.getDataType(a)
                if dtype == 'int' and a == 'id':
                    adict[a] = common_asset
                elif dtype == 'int':
                    adict[a] = int(firstrow[i])
                elif dtype == 'datetime':
                    dt = datetime.datetime.strptime(firstrow[i], '%Y-%m-%dT%H:%M:%S')
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    adict[a] = nd
                else:
                    adict[a] = firstrow[i]
        
        asset_query = 'CREATE (n:H_Asset) SET n = $dct'
        with self.driver.session() as session:
            session.run(asset_query, dct=adict)
        
        
        last_hwho = self.get_lastId('H_WhoProfile')
        last_swho = self.get_lastId('S_WhoProfile_schema')
        # last_lawho = self.get_lastId('L_Asset_WhoProfile')
        # last_luwho = self.get_lastId('L_WhoProfileUser')
        last_lawho = last_hwho
        last_luwho = last_hwho
        last_hhow = self.get_lastId('H_HowProfile')
        last_show = self.get_lastId('S_HowProfile_schema')
        # last_lhow = self.get_lastId('L_Asset_HowProfile')
        last_lhow = last_hhow
        last_hwhy = self.get_lastId('H_WhyProfile')
        last_swhy = self.get_lastId('S_WhyProfile_schema')
        # last_lwhy = self.get_lastId('L_Asset_WhyProfile')
        last_lwhy = last_hwhy
        last_hwhen = self.get_lastId('H_WhenProfile')
        last_swhen = self.get_lastId('S_WhenProfile_Attributes')
        #last_lwhen = self.get_lastId('L_Asset_WhenProfile')
        last_lwhen = last_swhen
        last_haction = self.get_lastId('H_Action')
        #last_laction = self.get_lastId('L_AssetsInActions')
        last_laction = last_haction
        
        hwho_inserts = self.get_q3inserts('H_WhoProfile', last_hwho, common_asset, 3, x)
        hhow_inserts = self.get_q3inserts('H_HowProfile', last_hhow, common_asset, 3, x)
        hwhy_inserts = self.get_q3inserts('H_WhyProfile', last_hwhy, common_asset, 3, x)
        hwhen_inserts = self.get_q3inserts('H_WhenProfile', last_hwhen, common_asset, 3, x)
        
        swho_inserts = self.get_q3links('S_WhoProfile_schema', last_swho, last_hwho, common_asset, 3, x)
        show_inserts = self.get_q3links('S_HowProfile_schema', last_show, last_hhow, common_asset, 3, x)
        swhy_inserts = self.get_q1links('S_WhyProfile_schema', last_swhy, last_hwhy, 3, x)
        swhen_inserts = self.get_q1links('S_WhenProfile_Attributes', last_swhen, last_hwhen, 3, x)
        
        lawho_inserts = self.get_q3links('L_Asset_WhoProfile', last_lawho, last_hwho, common_asset, 3, x)
        luwho_inserts = self.get_q3links('L_WhoProfileUser', last_luwho, last_hwho, common_asset, 3, x)
        lhow_inserts = self.get_q3links('L_Asset_HowProfile', last_lhow, last_hhow, common_asset, 3, x)
        lwhy_inserts = self.get_q3links('L_Asset_WhyProfile', last_lwhy, last_hwhy, common_asset, 3, x)
        lwhen_inserts = self.get_q3links('L_Asset_WhenProfile', last_lwhen, last_hwhen, common_asset, 3, x)
        
        haction_inserts = self.get_q3inserts('H_Action', last_haction, common_asset, 3, x)
        #def get_q3actions(self, tname, tid, hname, hid, asset_id, end_fk,end_id, qnum, x):
        laction_inserts = []
        action_who = self.get_q3actions('L_AssetsInActions', last_laction, 'action_id', 
                           last_haction, common_asset, 'who_profile_id', last_hwho, 3, x)
        action_how = self.get_q3actions('L_AssetsInActions', last_laction, 'action_id', 
                           last_haction, common_asset, 'how_profile_id', last_hhow, 3, x)
        action_why = self.get_q3actions('L_AssetsInActions', last_laction, 'action_id', 
                           last_haction, common_asset, 'why_profile_id', last_hwhy, 3, x)
        action_when = self.get_q3actions('L_AssetsInActions', last_laction, 'action_id', 
                           last_haction, common_asset, 'when_profile_id', last_hwhen, 3, x)
        action_assets = self.get_q3actions('L_AssetsInActions', last_laction, 'action_id', last_haction, common_asset, 'asset_id', common_asset, 3, x)
        laction_inserts.append(action_who)
        laction_inserts.append(action_how)
        laction_inserts.append(action_why)
        laction_inserts.append(action_when)
        laction_inserts.append(action_assets)
        
        who_links = {'H_WhoProfile' : {'L_Asset_WhoProfile' : [lawho_inserts], 
                                       'L_WhoProfileUser' : [luwho_inserts]}}
        how_links = {'H_HowProfile' : {'L_Asset_HowProfile' : [lhow_inserts]}}
        why_links = {'H_WhyProfile' : {'L_Asset_WhyProfile' : [lwhy_inserts]}}
        when_links = {'H_WhenProfile' : {'L_Asset_WhenProfile' : [lwhen_inserts]}}
        action_links = {'H_Action' : {'L_AssetsInActions' : laction_inserts}}
        #This is intended only for hubs! We need a separate func for satellites
    #linkdict--key is tname, value is another dictionary
    #key is lname, value is list of dicts, where each dict is the inserts
    #for that link
        self.execute_bulk_query('H_WhoProfile', hwho_inserts, who_links)
        self.execute_bulk_query('H_HowProfile', hhow_inserts, how_links)
        self.execute_bulk_query('H_WhyProfile', hwhy_inserts, why_links)
        self.execute_bulk_query('H_WhenProfile', hwhen_inserts, when_links)
        
        self.execute_sat_query('S_WhoProfile_schema', swho_inserts)
        self.execute_sat_query('S_HowProfile_schema', show_inserts)
        self.execute_sat_query('S_WhyProfile_schema', swhy_inserts)
        self.execute_sat_query('S_WhenProfile_Attributes', swhen_inserts)
        
        self.execute_bulk_query('H_Action', haction_inserts, action_links)
    
    def execute_q4(self, trial):
        query_str = 'MATCH (n:S_WhatProfile_schema)-[:Rel_S_WhatProfile_schema_H_WhatProfile]->(m:H_WhatProfile) '
        query_str += 'WHERE m.timestamp >= datetime({year: 2020, month: 12, day: 20}).epochMillis AND m.timestamp <= datetime({year: 2020, month: 11, day: 01}).epochMillis '
        query_str += ' RETURN n,m'
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q4_dneo4j_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q5(self, trial):
        query_str = 'MATCH (ac:H_Action)-[:Rel_H_Action_H_WhoProfile]->(who:H_WhoProfile)-'
        query_str += '[:Rel_L_WhoProfileUser_H_WhoProfile_H_User]->(u:H_User) '
        query_str += 'MATCH (ac:H_Action)-[:Rel_H_Action_H_WhyProfile]-(why:H_WhyProfile)-'
        query_str += '[:Rel_S_WhyProfile_schema_H_WhyProfile]-(swhy:S_WhyProfile_schema) '
        query_str += 'MATCH (ac:H_Action)-[:Rel_H_Action_H_WhenProfile]->(when:H_WhenProfile)-'
        query_str += '[:Rel_S_WhenProfile_Attributes_H_WhenProfile]-(swhen:S_WhenProfile_Attributes) '
        query_str += 'MATCH (ac:H_Action)-[:Rel_H_Action_H_HowProfile]->(how:H_HowProfile)-'
        query_str += '[:Rel_S_HowProfile_schema_H_HowProfile]-(show:S_HowProfile_schema)'
        query_str += 'RETURN u.name, show.schema, swhen.asset_timestamp, swhy.schema;'
        
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q5_dneo4j_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q6(self, trial):
        query_str = 'MATCH (ac:H_Action)-[:Rel_H_Action_H_WhoProfile]->(who:H_WhoProfile)-'
        query_str += '[:Rel_L_WhoProfileUser_H_WhoProfile_H_User]->(u:H_User) '
        query_str += 'MATCH (ac:H_Action)-[:Rel_H_Action_H_WhyProfile]-(why:H_WhyProfile)-'
        query_str += '[:Rel_S_WhyProfile_schema_H_WhyProfile]-(swhy:S_WhyProfile_schema) '
        query_str += 'MATCH (ac:H_Action)-[:Rel_H_Action_H_WhenProfile]->(when:H_WhenProfile)-'
        query_str += '[:Rel_S_WhenProfile_Attributes_H_WhenProfile]-(swhen:S_WhenProfile_Attributes) '
        query_str += 'MATCH (ac:H_Action)-[:Rel_H_Action_H_HowProfile]->(how:H_HowProfile)-'
        query_str += '[:Rel_S_HowProfile_schema_H_HowProfile]-(show:S_HowProfile_schema)'
        query_str += 'RETURN u.name, show.schema, swhen.asset_timestamp, swhy.schema '
        query_str += 'ORDER BY ac.timestamp DESC LIMIT 10;'
        
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q6_dneo4j_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q7(self, trial):
        query_str = 'MATCH (h:H_HowProfile)-[r:Rel_L_Asset_HowProfile_H_HowProfile_H_Asset]-(a:H_Asset) '
        query_str += 'RETURN a.id, count(r);'
        print("Executing Query 7:")
        print(query_str)
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q7_dneo4j_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_full(self):
        #q1 is complete
        #self.execute_q1()
        #q2 is complete
        #self.execute_q2()
        #q3 is complete
        #self.execute_q3(100000)
        # self.execute_q4(0)
        # self.execute_q5(0)
        # self.execute_q6(0)
        #self.execute_q7(0)
        for i in range(6):
            # os.system('echo 1 | sudo tee /proc/sys/vm/drop_caches')
            time.sleep(5)
            # self.execute_q4(i)
            # self.execute_q5(i)
            # self.execute_q6(i)
            self.execute_q7(i)
            print("Finished executing Trial: " + str(i))
        
if __name__ == "__main__":
    neo_queries = DNeo4j_Queries("bolt://localhost:7687", "neo4j", "normal")
    neo_queries.execute_full()