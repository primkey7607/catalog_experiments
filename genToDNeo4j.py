from neo4j import GraphDatabase
import os
import datetime
from neo4j.time import DateTime
import random

class GenNNeo4j:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.attrmap = {}
        self.init_attrmap()
        self.version = 1
    
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
    
    def getDataType(self, attrName):
        if 'id' in attrName:
            return 'int'
        elif attrName == 'version':
            return 'int'
        elif 'timestamp' in attrName or 'date' in attrName:
            return 'datetime'
        else:
            return 'string'
    
    def create_relquery(self, t1, t2, k1, k2):
        #sample: MATCH (a:Person),(b:Person)
        #WHERE a.name = 'A' AND b.name = 'B'
        #CREATE (a)-[r:RELTYPE]->(b)
        #RETURN type(r)
        query_str = "MATCH (a:" + t1 + "), (b:" + t2 + ") "
        query_str += "WHERE a." + k1 + " = b." + k2
        query_str += " CREATE (a)-[r:RELTYPE]->(b);"
        return query_str
    
    def insert_linkRel(self, t1, t2, k1, k2, lname, l_recs):
        query_str = "UNWIND $props AS map MATCH (a:" + t1 + "), (b:" + t2 + ") "
        query_str += "WHERE a." + k1 + " = b." + k2
        query_str += " CREATE (a)-[r:" + lname + "]->(b) SET r = map;"
        
        with self.driver.session() as session:
            session.run(query_str, props=l_recs)
    
    def make_query(self, tname):
        query_str = "UNWIND $props AS map CREATE (n:" + tname + ") SET n = map"
        return query_str
    
    def insertToDB(self, tname, query_str, recs):
        recmap = []
        attrs = self.attrmap[tname]
        for r in recs:
            tmpdict = {}
            for i,a in enumerate(attrs):
                dtype = self.getDataType(a)
                if dtype == 'int':
                    tmpdict[a] = int(r[i])
                elif dtype == 'datetime':
                    dt = datetime.datetime.strptime(r[i], '%Y-%m-%d %H:%M:%S')
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    tmpdict[a] = nd
                else:
                    tmpdict[a] = r[i]
            recmap.append(tmpdict)
        
        with self.driver.session() as session:
            session.run(query_str, props=recmap)
    
    def random_date(self):
        start = datetime.datetime.strptime('6/1/2020 12:00 AM', '%m/%d/%Y %I:%M %p')
        end = datetime.datetime.now()
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + datetime.timedelta(seconds=random_second)
    
    def init_keymap(self):
        self.keymap["H_UserType"] = 0
        self.keymap["H_User"] = 0
        self.keymap["H_AssetType"] = 0
        self.keymap["H_Asset"] = 0
        self.keymap["H_WhoProfile"] = 0
        self.keymap["H_WhatProfile"] = 0
        self.keymap["H_HowProfile"] = 0
        self.keymap["H_WhyProfile"] = 0
        self.keymap["H_WhenProfile"] = 0
        self.keymap["H_SourceType"] = 0
        self.keymap["H_Source"] = 0
        self.keymap["H_WhereProfile"] = 0
        self.keymap["H_Action"] = 0
        self.keymap["H_RelationshipType"] = 0
        self.keymap["H_Relationship"] = 0
        self.keymap["L_UserTypeLink"] = 0
        self.keymap["L_AssetTypeLink"] = 0
        self.keymap["L_Asset_WhoProfile"] = 0
        self.keymap["L_WhoProfileUser"] = 0
        self.keymap["L_Asset_HowProfile"] = 0
        self.keymap["L_Asset_WhyProfile"] = 0
        self.keymap["L_Asset_WhatProfile"] = 0
        self.keymap["L_Asset_WhenProfile"] = 0
        self.keymap["L_Source2Type"] = 0
        self.keymap["L_Asset_WhereProfile"] = 0
        self.keymap["L_AssetsInActions"] = 0
        self.keymap["L_Relationship_Type"] = 0
        self.keymap["L_Asset_Relationships"] = 0
        self.keymap["S_User_schema"] = 0
        self.keymap["S_WhoProfile_schema"] = 0
        self.keymap["S_HowProfile_schema"] = 0
        self.keymap["S_WhyProfile_schema"] = 0
        self.keymap["S_WhatProfile_schema"] = 0
        self.keymap["S_WhenProfile_Attributes"] = 0
        self.keymap["S_Configuration"] = 0
        self.keymap["S_SourceTypeAttributes"] = 0
        self.keymap["S_AssetTypeAttributes"] = 0
        self.keymap["S_UserTypeAttributes"] = 0
        self.keymap["S_RelationshipTypeAttributes"] = 0
        self.keymap["S_Relationship_schema"] = 0
        self.keymap["S_Source_schema"] = 0
        self.keymap["L_WhereProfile_Source"] = 0
    
    #there's pretty much no way to factor this nicely...
    def load_usertype(self):
        h_usertype_recs = []
        s_usertype_recs = []
        
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_user_query = self.make_query('H_UserType')
        s_user_query = self.make_query('S_UserTypeAttributes')
        with open('UserType.csv', 'r') as fh:
            for i,row in enumerate(fh):
                rowlen = len(row)
                if rowlen == 0:
                    continue
                utype_id = row[0]
                utype_name = row[1]
                utype_desc = row[2]
                timestamp = self.random_date()
                user_id = 1
                h_usertype_recs.append([utype_id, str(self.version),
                                        timestamp, user_id])
                s_usertype_recs.append([utype_id, utype_name, utype_desc,
                                        str(self.version), timestamp,
                                        user_id])
                h_chunksize += 1
                s_chunksize += 1
                if h_chunksize >= 1000:
                    h_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting h_Chunk Number: " + str(h_chunknum))
                    self.insertToDB('H_UserType', h_user_query, h_usertype_recs)
                    h_usertype_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToDB('S_UserTypeAttributes', s_user_query, s_usertype_recs)
                    s_usertype_recs = []
                    s_chunksize = 0
            
                #insert any leftovers:
            if len(h_usertype_recs) > 0:
                self.insertToDB('H_UserType', h_user_query, h_usertype_recs)
            if len(s_usertype_recs) > 0:
                self.insertToDB('S_UserTypeAttributes', s_user_query, s_usertype_recs)
    
    def load_assettype(self):
        h_assettype_recs = []
        s_assettype_recs = []
        
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_a_query = self.make_query('H_AssetType')
        s_a_query = self.make_query('S_AssetTypeAttributes')
        with open('AssetType.csv', 'r') as fh:
            for i,row in enumerate(fh):
                atype_id = row[0]
                atype_name = row[1]
                atype_desc = row[2]
                timestamp = self.random_date()
                user_id = 1
                h_assettype_recs.append([atype_id, str(self.version),
                                         timestamp, user_id])
                s_assettype_recs.append([atype_id, atype_name, atype_desc,
                                         str(self.version), timestamp, user_id])
                h_chunksize += 1
                s_chunksize += 1
                if h_chunksize >= 1000:
                    h_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting h_Chunk Number: " + str(h_chunknum))
                    self.insertToDB('H_AssetType', h_a_query, h_assettype_recs)
                    h_assettype_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToDB('S_AssetTypeAttributes', s_a_query, s_assettype_recs)
                    s_assettype_recs = []
                    s_chunksize = 0
            
            #insert any leftovers:
            if len(h_assettype_recs) > 0:
                self.insertToDB('H_AssetType', h_a_query, h_assettype_recs)
            if len(s_assettype_recs) > 0:
                self.insertToDB('S_AssetTypeAttributes', s_a_query, s_assettype_recs)
    
    def load_reltype(self):
        h_relationshiptype_recs = []
        s_relationshiptype_recs = []
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_r_query = self.make_query('H_RelationshipType')
        s_r_query = self.make_query('S_RelationshipTypeAttributes')
        with open('RelationshipType.csv', 'r') as fh:
            for i, row in enumerate(fh):
                pid = row[0]
                name = row[1]
                desc = row[2]
                timestamp = self.random_date()
                user_id = 1
                h_relationshiptype_recs.append([pid, self.version, timestamp, user_id])
                s_relationshiptype_recs.append([pid, name, desc, self.version, timestamp, user_id])
                h_chunksize += 1
                s_chunksize += 1
                if h_chunksize >= 1000:
                    h_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting h_Chunk Number: " + str(h_chunknum))
                    self.insertToDB('H_RelationshipType', h_r_query, h_relationshiptype_recs)
                    h_relationshiptype_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToDB('S_RelationshipTypeAttributes', s_r_query, s_relationshiptype_recs)
                    s_relationshiptype_recs = []
                    s_chunksize = 0
            
            #insert any leftovers:
            if len(h_relationshiptype_recs) > 0:
                self.insertToDB('H_RelationshipType', h_r_query, h_relationshiptype_recs)
            if len(s_relationshiptype_recs) > 0:
                self.insertToDB('S_RelationshipTypeAttributes', s_r_query, s_relationshiptype_recs)
        
    
    def load_sourcetype(self):
        h_sourcetype_recs = []
        s_sourcetype_recs = []
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_s_query = self.make_query('H_SourceType')
        s_s_query = self.make_query('S_SourceTypeAttributes')
        with open('SourceType.csv', 'r') as fh:
            for i,row in enumerate(fh):
                pid = row[0]
                connector = row[1]
                serde = row[2]
                datamodel = row[3]
                timestamp = self.random_date()
                user_id = 1
                h_sourcetype_recs.append([pid, self.version, timestamp, user_id])
                s_sourcetype_recs.append([pid, pid, connector, serde, datamodel,
                                         self.version, timestamp, user_id])
                h_chunksize += 1
                s_chunksize += 1
                if h_chunksize >= 1000:
                    h_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting h_Chunk Number: " + str(h_chunknum))
                    self.insertToDB('H_SourceType', h_s_query, h_sourcetype_recs)
                    h_sourcetype_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToDB('S_SourceTypeAttributes', s_s_query, s_sourcetype_recs)
                    s_sourcetype_recs = []
                    s_chunksize = 0
            
            #insert any leftovers:
            if len(h_sourcetype_recs) > 0:
                self.insertToDB('H_SourceType', h_s_query, h_sourcetype_recs)
            if len(s_sourcetype_recs) > 0:
                self.insertToDB('S_SourceTypeAttributes', s_s_query, s_sourcetype_recs)
    
    def load_users(self):
        h_user_recs = []
        s_user_recs = []
        l_user_recs = []
        
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_query = self.make_query('H_User')
        s_query = self.make_query('S_User_schema')
        l_query = self.make_query('L_UserTypeLink')
        with open('User.csv', 'r') as fh:
            for i,row in enumerate(fh):
                pid = row[0]
                name = row[1]
                utype = row[2]
                schema = row[3]
                ver = row[4]
                date = row[5]
                user_id = row[6]
                h_user_recs.append([pid, name, ver, date, user_id])
                s_user_recs.append([pid, pid, schema, ver, date, user_id])
                self.keymap['L_UserTypeLink'] += 1
                l_user_recs.append([self.keymap['L_UserTypeLink'], pid, utype,
                                   self.version, self.random_date(), user_id])
                h_chunksize += 1
                s_chunksize += 1
                l_chunksize += 1
                if h_chunksize >= 1000:
                    h_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting h_Chunk Number: " + str(h_chunknum))
                    self.insertToDB('H_User', h_query, h_user_recs)
                    h_user_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToDB('S_User_schema', s_query, s_user_recs)
                    s_user_recs = []
                    s_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToDB('L_UserTypeLink', l_query, l_user_recs)
                    l_user_recs = []
                    l_chunksize = 0
            
            #insert any leftovers:
            if len(h_user_recs) > 0:
                self.insertToDB('H_User', h_query, h_user_recs)
            if len(s_user_recs) > 0:
                self.insertToDB('S_User_schema', s_query, s_user_recs)
            if len(l_user_recs) > 0:
                self.insertToDB('L_UserTypeLink', l_query, l_user_recs)
    
    def load_assets(self):
        h_recs = []
        l_recs = []
        
        h_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        h_query = self.make_query('h_asset')
        l_query = self.make_query('l_assettypelink')
        for i,row in enumerate(self.csv_map['Asset']):
            pid = row[0]
            name = row[1]
            atype = row[2]
            ver = row[3]
            date = row[4]
            user_id = row[5]
            h_recs.append([pid, name, ver, date, user_id])
            self.keymap['L_AssetTypeLink'] += 1
            l_recs.append([self.keymap['L_AssetTypeLink'],
                          pid, atype, self.version, self.random_date(), user_id])
            h_chunksize += 1
            l_chunksize += 1
            if h_chunksize >= 1000:
                h_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting h_Chunk Number: " + str(h_chunknum))
                self.insertToDB(self.con, h_query, h_recs)
                h_recs = []
                h_chunksize = 0
            if l_chunksize >= 1000:
                l_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(l_chunknum))
                self.insertToDB(self.con, l_query, l_recs)
                l_recs = []
                l_chunksize = 0
        
        #insert any leftovers:
        if len(h_recs) > 0:
            self.insertToDB(self.con, h_query, h_recs)
        if len(l_recs) > 0:
            self.insertToDB(self.con, l_query, l_recs)

