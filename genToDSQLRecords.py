from backends.datavault_models import *
import csv
import datetime
import random
import string
import sys
import sqlite3
#NOTE: we only generate records here!
#specifically, we generate them from the same csv files
#as the ones we generated for the normalized schema
class GenDSQLRecords:
    
    def __init__(self):
        self.version = 1
        self.con = None
        self.asset_names = []
        #self.ins_map = {}
        #self.create_tables()
        self.keymap = {}
        self.init_keymap()
        self.csv_map = {}
        self.fh_map = {}
        self.create_readers()
        self.common_props = {}
        self.init_props()
    
    #we can use the same properties here
    def init_props(self):
        self.common_props['UserType'] = ['administrator', 'creator', 'curator',
                                         'analyst', 'db_admin']
        self.common_props['User'] = ['address', 'ip_address', 'email', 'code_repo',
                                     'profile_url', 'expertise', 'reputation']
        self.common_props['AssetType'] = ['table', 'column', 'document', 'image']
        self.common_props['WhoProfile'] = ['access_privilege', 'asset_role']
        self.common_props['WhatProfile_num'] = ['mean', 'median', 'mode', 'std_dev',
                                                'range', 'minhash', 'size', '']
        self.common_props['WhatProfile_text'] = ['datatype', 'attribute_meaning',
                                                 'instance_meaning', 'pii']
        self.common_props['HowProfile'] = ['script_url', 'preprocessing_steps',
                                           'analysis_program', 'collection_method',
                                           'preparation_method']
        self.common_props['WhyProfile'] = ['Motivation', 'Intended_uses', 'prohibited_uses',
                                           'creation_reason', 'analysis_use_case']
        self.common_props['Source'] = ['Repo_Access_Privileges', 'credentials_required',
                                       'site_reputation', 'security_measures']
        self.common_props['RelationshipType'] = ['sameAs', 'moreRecentThan', 'olderVersionOf',
                                                 'joinOf', 'sharesKeyWith', 'sameImageAs',
                                                 'sameTopicAs']
        self.common_props['Relationship'] = ['sameAs', 'moreRecentThan', 'olderVersionOf',
                                                 'joinOf', 'sharesKeyWith', 'sameImageAs',
                                                 'sameTopicAs']
        self.common_props['SourceType_connector'] = ['FS', 'DB connection', 'Web repo']
        self.common_props['SourceType_serde'] = ['csv', 'parquet', 'postgres', 'hive',
                                                'mongodb', 'SQL server',
                                                'SQLite', 'Web repo']
        self.common_props['SourceType_datamodel'] = ['row-oriented', 'column-oriented']
        self.common_props['WhereProfile_conf'] = ['distribution', 'encodingFormat']
    
    def create_tables(self):
        database.init('datavault_synthetic.db')
        with database:
            database.create_tables([H_UserType, H_User, H_AssetType,
                                    H_Asset, H_WhoProfile, H_WhatProfile,
                                    H_HowProfile, H_WhyProfile, H_WhenProfile,
                                    H_SourceType, H_Source, H_WhereProfile, 
                                    H_Action, H_RelationshipType, H_Relationship,
                                    L_UserTypeLink, L_AssetTypeLink, L_Asset_WhoProfile,
                                    L_WhoProfileUser, L_Asset_HowProfile, L_Asset_WhyProfile,
                                    L_Asset_WhatProfile, L_Asset_WhenProfile, L_Source2Type,
                                    L_Asset_WhereProfile, L_AssetsInActions, L_Relationship_Type,
                                    L_Asset_Relationships, S_User_schema, S_WhoProfile_schema,
                                    S_HowProfile_schema, S_WhyProfile_schema, S_WhatProfile_schema,
                                    S_WhenProfile_Attributes, S_Configuration, S_SourceTypeAttributes,
                                    S_AssetTypeAttributes, S_UserTypeAttributes,
                                    S_RelationshipTypeAttributes, S_Relationship_schema,
                                    S_Source_schema, L_WhereProfile_Source])
            self.ins_map["H_UserType"] = H_UserType
            self.ins_map["H_User"] = H_User
            self.ins_map["H_AssetType"] = H_AssetType
            self.ins_map["H_Asset"] = H_Asset
            self.ins_map["H_WhoProfile"] = H_WhoProfile
            self.ins_map["H_WhatProfile"] = H_WhatProfile
            self.ins_map["H_HowProfile"] = H_HowProfile
            self.ins_map["H_WhyProfile"] = H_WhyProfile
            self.ins_map["H_WhenProfile"] = H_WhenProfile
            self.ins_map["H_SourceType"] = H_SourceType
            self.ins_map["H_Source"] = H_Source
            self.ins_map["H_WhereProfile"] = H_WhereProfile
            self.ins_map["H_Action"] = H_Action
            self.ins_map["H_RelationshipType"] = H_RelationshipType
            self.ins_map["H_Relationship"] = H_Relationship
            self.ins_map["L_UserTypeLink"] = L_UserTypeLink
            self.ins_map["L_AssetTypeLink"] = L_AssetTypeLink
            self.ins_map["L_Asset_WhoProfile"] = L_Asset_WhoProfile
            self.ins_map["L_WhoProfileUser"] = L_WhoProfileUser
            self.ins_map["L_Asset_HowProfile"] = L_Asset_HowProfile
            self.ins_map["L_Asset_WhyProfile"] = L_Asset_WhyProfile
            self.ins_map["L_Asset_WhatProfile"] = L_Asset_WhatProfile
            self.ins_map["L_Asset_WhenProfile"] = L_Asset_WhenProfile
            self.ins_map["L_Source2Type"] = L_Source2Type
            self.ins_map["L_Asset_WhereProfile"] = L_Asset_WhereProfile
            self.ins_map["L_AssetsInActions"] = L_AssetsInActions
            self.ins_map["L_Relationship_Type"] = L_Relationship_Type
            self.ins_map["L_Asset_Relationships"] = L_Asset_Relationships
            self.ins_map["S_User_schema"] = S_User_schema
            self.ins_map["S_WhoProfile_schema"] = S_WhoProfile_schema
            self.ins_map["S_HowProfile_schema"] = S_HowProfile_schema
            self.ins_map["S_WhyProfile_schema"] = S_WhyProfile_schema
            self.ins_map["S_WhatProfile_schema"] = S_WhatProfile_schema
            self.ins_map["S_WhenProfile_Attributes"] = S_WhenProfile_Attributes
            self.ins_map["S_Configuration"] = S_Configuration
            self.ins_map["S_SourceTypeAttributes"] = S_SourceTypeAttributes
            self.ins_map["S_AssetTypeAttributes"] = S_AssetTypeAttributes
            self.ins_map["S_UserTypeAttributes"] = S_UserTypeAttributes
            self.ins_map["S_RelationshipTypeAttributes"] = S_RelationshipTypeAttributes
            self.ins_map["S_Relationship_schema"] = S_Relationship_schema
            self.ins_map["S_Source_schema"] = S_Source_schema
            self.ins_map["L_WhereProfile_Source"] = L_WhereProfile_Source
            
            H_User.insert({"name": "admin", 
                      "version": self.version, "timestamp" : str(datetime.datetime.now())}).execute()
            
            H_UserType.insert({"version" : self.version, "timestamp" : str(datetime.datetime.now())}).execute()
            
            S_UserTypeAttributes.insert({"version" : self.version, "timestamp" : str(datetime.datetime.now()),
                                         "name" : "administrator",
                                         "description" : "responsible for populating the catalog"}).execute()
            
            
            S_User_schema.insert({"schema": {"addr":{"home":"westlake","company":"bank"},"phone":1234567},
                                  "version": self.version, "timestamp" : str(datetime.datetime.now()),
                                  "user" : 1}).execute()
            
            #now, link the tables together
            L_UserTypeLink.insert({"write_user" : 1, "user_type" : 1,
                                   "version" : self.version,
                                   "timestamp" : str(datetime.datetime.now())}).execute()
    
    def create_readers(self):
        #normalized names
        norm_map = {}
        norm_map['UserType'] = ['name', 'description']
        norm_map['User'] = ['name', 'user_type', 'schema',
                                'version', 'timestamp', 'user']
        norm_map['AssetType'] = ['name', 'description']
        norm_map['Asset'] = ['name', 'asset_type', 'version',
                                 'timestamp', 'user']
        norm_map['WhoProfile'] = ['version', 'timestamp', 'write_user',
                                      'asset', 'user', 'schema']
        norm_map['WhatProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'schema']
        norm_map['HowProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'schema']
        norm_map['WhyProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'schema']
        norm_map['WhenProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'asset_timestamp',
                                      'expiry_date', 'start_date']
        norm_map['SourceType'] = ['connector', 'serde', 'datamodel']
        norm_map['Source'] = ['version', 'timestamp', 'user',
                                  'name', 'source_type', 'schema']
        norm_map['WhereProfile'] = ['version', 'timestamp', 'user',
                                        'asset', 'access_path', 'source', 'configuration']
        norm_map['RelationshipType'] = ['name', 'description']
        norm_map['Relationship'] = ['version', 'timestamp', 'user',
                                        'relationship_type', 'schema']
        norm_map['Asset_Relationships'] = ['asset', 'relationship']
        norm_map['Action'] = ['version', 'timestamp', 'user', 'timestamp',
                                  'asset', 'who', 'how', 'why', 'when']
        for key in norm_map:
            fh = open(key + '.csv', 'r')
            
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            self.fh_map[key] = fh
            self.csv_map[key] = csvreader
            #self.csv_map[key].writeheader()
    
    def random_date(self):
        start = datetime.datetime.strptime('6/1/2020 12:00 AM', '%m/%d/%Y %I:%M %p')
        end = datetime.datetime.now()
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + datetime.timedelta(seconds=random_second)
    
    def k_in_time(self, start, end, k):
        delta = end - start
        klst = []
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        for i in range(k):
            random_second = random.randrange(int_delta)
            klst.append(random_second)
        
        klst.sort()
        res_dates = []
        for s in klst:
            cur_date = start + datetime.timedelta(seconds=s)
            res_dates.append(cur_date)
            
        return res_dates
    
    def init_keymap(self):
        self.keymap["H_UserType"] = 1
        self.keymap["H_User"] = 1
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
        self.keymap["L_UserTypeLink"] = 1
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
        self.keymap["S_User_schema"] = 1
        self.keymap["S_WhoProfile_schema"] = 0
        self.keymap["S_HowProfile_schema"] = 0
        self.keymap["S_WhyProfile_schema"] = 0
        self.keymap["S_WhatProfile_schema"] = 0
        self.keymap["S_WhenProfile_Attributes"] = 0
        self.keymap["S_Configuration"] = 0
        self.keymap["S_SourceTypeAttributes"] = 0
        self.keymap["S_AssetTypeAttributes"] = 0
        self.keymap["S_UserTypeAttributes"] = 1
        self.keymap["S_RelationshipTypeAttributes"] = 0
        self.keymap["S_Relationship_schema"] = 0
        self.keymap["S_Source_schema"] = 0
        self.keymap["L_WhereProfile_Source"] = 0
    
    def make_query(self, tname):
        query = 'INSERT INTO ' + tname.lower() + ' VALUES ('
        con = sqlite3.connect('/home/pranav/catalog_experiments/datavault_synthetic.db')
        cur = con.cursor()
        schema = cur.execute("PRAGMA table_info('" + tname + "');").fetchall()
        con.close()
        rowlen = len(schema)
        for i in range(rowlen):
            query += '?,'
        query = query[:-1] + ');'
        return query
    
    #do this first, before creating records for other tables
    def create_type_records(self):
        h_usertype_recs = []
        s_usertype_recs = []
        
        h_assettype_recs = []
        s_assettype_recs = []
        
        h_relationshiptype_recs = []
        s_relationshiptype_recs = []
        
        h_sourcetype_recs = []
        s_sourcetype_recs = []
        
        #do UserType first
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_user_query = self.make_query('h_usertype')
        s_user_query = self.make_query('s_usertypeattributes')
        for i,row in enumerate(self.csv_map['UserType']):
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
                self.insertToDB(self.con, h_user_query, h_usertype_recs)
                h_usertype_recs = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_user_query, s_usertype_recs)
                s_usertype_recs = []
                s_chunksize = 0
        
        #insert any leftovers:
        if len(h_usertype_recs) > 0:
            self.insertToDB(self.con, h_user_query, h_usertype_recs)
        if len(s_usertype_recs) > 0:
            self.insertToDB(self.con, s_user_query, s_usertype_recs)
            
            
        # res_inserts.append((self.make_query('h_usertype'), h_usertype_recs))
        # res_inserts.append((self.make_query('s_usertypeattributes'), s_usertype_recs))
            
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_a_query = self.make_query('h_assettype')
        s_a_query = self.make_query('s_assettypeattributes')
        for i,row in enumerate(self.csv_map['AssetType']):
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
                self.insertToDB(self.con, h_a_query, h_assettype_recs)
                h_assettype_recs = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_a_query, s_assettype_recs)
                s_assettype_recs = []
                s_chunksize = 0
        
        #insert any leftovers:
        if len(h_assettype_recs) > 0:
            self.insertToDB(self.con, h_a_query, h_assettype_recs)
        if len(s_assettype_recs) > 0:
            self.insertToDB(self.con, s_a_query, s_assettype_recs)
            
        # res_inserts.append((self.make_query('h_assettype'), h_assettype_recs))
        # res_inserts.append((self.make_query('s_assettypeattributes'), s_assettype_recs))
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_r_query = self.make_query('h_relationshiptype')
        s_r_query = self.make_query('s_relationshiptypeattributes')
        for row in self.csv_map['RelationshipType']:
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
                self.insertToDB(self.con, h_r_query, h_relationshiptype_recs)
                h_relationshiptype_recs = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_r_query, s_relationshiptype_recs)
                s_relationshiptype_recs = []
                s_chunksize = 0
        
        #insert any leftovers:
        if len(h_assettype_recs) > 0:
            self.insertToDB(self.con, h_r_query, h_relationshiptype_recs)
        if len(s_assettype_recs) > 0:
            self.insertToDB(self.con, s_r_query, s_relationshiptype_recs)
        
        # res_inserts.append((self.make_query('h_relationshiptype'), h_relationshiptype_recs))
        # res_inserts.append((self.make_query('s_relationshiptypeattributes'), s_relationshiptype_recs))
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_s_query = self.make_query('h_sourcetype')
        s_s_query = self.make_query('s_sourcetypeattributes')
        for row in self.csv_map['SourceType']:
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
                self.insertToDB(self.con, h_s_query, h_sourcetype_recs)
                h_sourcetype_recs = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_s_query, s_sourcetype_recs)
                s_sourcetype_recs = []
                s_chunksize = 0
        
        #insert any leftovers:
        if len(h_sourcetype_recs) > 0:
            self.insertToDB(self.con, h_s_query, h_sourcetype_recs)
        if len(s_sourcetype_recs) > 0:
            self.insertToDB(self.con, s_s_query, s_sourcetype_recs)
            
        # res_inserts.append((self.make_query('h_sourcetype'), h_sourcetype_recs))
        # res_inserts.append((self.make_query('s_sourcetypeattributes'), s_sourcetype_recs))
        
        # return res_inserts
        
        
    #create all users, as before
    def create_all_users(self):
        h_user_recs = []
        s_user_recs = []
        l_user_recs = []
        
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_query = self.make_query('h_user')
        s_query = self.make_query('s_user_schema')
        l_query = self.make_query('l_usertypelink')
        for i,row in enumerate(self.csv_map['User']):
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
                self.insertToDB(self.con, h_query, h_user_recs)
                h_user_recs = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_query, s_user_recs)
                s_user_recs = []
                s_chunksize = 0
            if l_chunksize >= 1000:
                l_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(l_chunknum))
                self.insertToDB(self.con, l_query, l_user_recs)
                l_user_recs = []
                l_chunksize = 0
        
        #insert any leftovers:
        if len(h_user_recs) > 0:
            self.insertToDB(self.con, h_query, h_user_recs)
        if len(s_user_recs) > 0:
            self.insertToDB(self.con, s_query, s_user_recs)
        if len(l_user_recs) > 0:
            self.insertToDB(self.con, l_query, l_user_recs)
            
        # res_inserts.append((self.make_query('h_user'), h_user_recs))
        # res_inserts.append((self.make_query('s_user_schema'), s_user_recs))
        # res_inserts.append((self.make_query('l_usertypelink'), l_user_recs))
            
        # return res_inserts
    
    def create_all_assets(self):
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
        # res_inserts.append((self.make_query('h_asset'), h_recs))
        # res_inserts.append((self.make_query('l_assettypelink'), l_recs))
        # return res_inserts
    
    def create_all_sources(self):
        h_recs = []
        l_recs = []
        s_recs = []
        
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_query = self.make_query('h_source')
        l_query = self.make_query('l_source2type')
        s_query = self.make_query('s_source_schema')
        
        for i,row in enumerate(self.csv_map['Source']):
            pid = row[0]
            ver = row[1]
            date = row[2]
            uid = row[3]
            name = row[4]
            s_type = row[5]
            schema = row[6]
            
            h_recs.append([pid, name, ver, date, uid])
            s_recs.append([pid, schema, ver, date, uid])
            self.keymap['L_Source2Type'] += 1
            l_recs.append([self.keymap['L_Source2Type'], pid, s_type,
                           ver, date, uid])
            h_chunksize += 1
            l_chunksize += 1
            s_chunksize += 1
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
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_query, s_recs)
                s_recs = []
                s_chunksize = 0
        
        #insert any leftovers:
        if len(h_recs) > 0:
            self.insertToDB(self.con, h_query, h_recs)
        if len(l_recs) > 0:
            self.insertToDB(self.con, l_query, l_recs)
        if len(s_recs) > 0:
            self.insertToDB(self.con, s_query, s_recs)
            
        # res_inserts.append((self.make_query('h_source'), h_recs))
        # res_inserts.append((self.make_query('l_source2type'), l_recs))
        # res_inserts.append((self.make_query('s_source_schema'), s_recs))
        # return res_inserts
    
    def create_all_relationships(self):
        h_recs = []
        l_recs = []
        l_ar = []
        s_recs = []
        uids = []
        
        # h_chunksize = 0
        # s_chunksize = 0
        # l_chunksize = 0
        la_chunksize = 0
        # h_chunknum = 0
        # l_chunknum = 0
        la_chunknum = 0
        # s_chunknum = 0
        # h_query = self.make_query('h_relationship')
        # l_query = self.make_query('l_relationship_type')
        # s_query = self.make_query('s_relationship_schema')
        la_query = self.make_query('l_asset_relationships')
        for i,row in enumerate(self.csv_map['Relationship']):
        #     pid = row[0]
        #     ver = row[1]
        #     date = row[2]
            uid = row[3]
            #print("Appending UID number: " + str(i))
            uids.append(uid)
        #     r_type = row[4]
        #     schema = row[5]
        #     h_recs.append([pid, ver, date, uid])
        #     s_recs.append([pid, pid, schema, ver, date, uid])
        #     self.keymap["L_Relationship_Type"] += 1
        #     l_recs.append([self.keymap['L_Relationship_Type'],
        #                    pid, r_type, ver, date, uid])
        #     h_chunksize += 1
        #     l_chunksize += 1
        #     s_chunksize += 1
        #     if h_chunksize >= 1000:
        #         h_chunknum += 1
        #         print("Inserting through row: " + str(i))
        #         print("Inserting h_Chunk Number: " + str(h_chunknum))
        #         self.insertToDB(self.con, h_query, h_recs)
        #         h_recs = []
        #         h_chunksize = 0
        #     if l_chunksize >= 1000:
        #         l_chunknum += 1
        #         print("Inserting through row: " + str(i))
        #         print("Inserting l_Chunk Number: " + str(l_chunknum))
        #         self.insertToDB(self.con, l_query, l_recs)
        #         l_recs = []
        #         l_chunksize = 0
        #     if s_chunksize >= 1000:
        #         s_chunknum += 1
        #         print("Inserting through row: " + str(i))
        #         print("Inserting s_Chunk Number: " + str(s_chunknum))
        #         self.insertToDB(self.con, s_query, s_recs)
        #         s_recs = []
        #         s_chunksize = 0
        
        # #insert any leftovers:
        # if len(h_recs) > 0:
        #     self.insertToDB(self.con, h_query, h_recs)
        # if len(l_recs) > 0:
        #     self.insertToDB(self.con, l_query, l_recs)
        # if len(s_recs) > 0:
        #     self.insertToDB(self.con, s_query, s_recs)
            
        for i,row in enumerate(self.csv_map['Asset_Relationships']):
            #print("Considering row: " + str(i))
            pid = row[0]
            asset_id = row[1]
            r_id = row[2]
            ver = self.version
            date = self.random_date()
            uid = random.choice(uids)
            self.keymap['L_Asset_Relationships'] += 1
            l_ar.append([self.keymap['L_Asset_Relationships'],
                         asset_id, r_id, ver, date, uid])
            la_chunksize += 1
            if la_chunksize >= 1000:
                la_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(la_chunknum))
                self.insertToDB(self.con, la_query, l_ar)
                l_ar = []
                la_chunksize = 0
        
        #insert any leftovers
        if len(l_ar) > 0:
            self.insertToDB(self.con, la_query, l_ar)
        
        # res_inserts.append((self.make_query('h_relationship'), h_recs))
        # res_inserts.append((self.make_query('s_relationship_schema'), s_recs))
        # res_inserts.append((self.make_query('l_relationship_type'), l_recs))
        # res_inserts.append((self.make_query('l_asset_relationships'), l_ar))
        # return res_inserts
    
    def create_records_per_table(self):
        h_who = []
        s_who = []
        l_who = []
        l_user = []
        h_chunksize = 0
        s_chunksize = 0
        la_chunksize = 0
        lu_chunksize = 0
        h_chunknum = 0
        la_chunknum = 0
        lu_chunknum = 0
        s_chunknum = 0
        h_who_query = self.make_query('h_whoprofile')
        s_who_query = self.make_query('s_whoprofile_schema')
        l_awho_query = self.make_query('l_asset_whoprofile')
        l_uwho_query = self.make_query('l_whoprofileuser')
        for i,row in enumerate(self.csv_map['WhoProfile']):
            pid = row[0]
            ver = row[1]
            date = row[2]
            write_user = row[3]
            asset_id = row[4]
            uid = row[5]
            schema = row[6]
            h_who.append([pid, ver, date, write_user])
            s_who.append([pid, pid, schema, uid, ver, date, write_user])
            self.keymap['L_Asset_WhoProfile'] += 1
            l_who.append([self.keymap['L_Asset_WhoProfile'], asset_id, pid,
                          ver, date, write_user])
            self.keymap['L_WhoProfileUser'] += 1
            l_user.append([self.keymap['L_WhoProfileUser'],
                           pid, uid, ver, date, write_user])
            h_chunksize += 1
            s_chunksize += 1
            la_chunksize += 1
            lu_chunksize += 1
            if h_chunksize >= 1000:
                h_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting h_Chunk Number: " + str(h_chunknum))
                self.insertToDB(self.con, h_who_query, h_who)
                h_who = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_who_query, s_who)
                s_who = []
                s_chunksize = 0
            if la_chunksize >= 1000:
                la_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(la_chunknum))
                self.insertToDB(self.con, l_awho_query, l_who)
                l_who = []
                la_chunksize = 0
            if lu_chunksize >= 1000:
                lu_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(lu_chunknum))
                self.insertToDB(self.con, l_uwho_query, l_user)
                l_user = []
                lu_chunksize = 0
        
        #insert any leftovers:
        if len(h_who) > 0:
            self.insertToDB(self.con, h_who_query, h_who)
        if len(s_who) > 0:
            self.insertToDB(self.con, s_who_query, s_who)
        if len(l_who) > 0:
            self.insertToDB(self.con, l_awho_query, l_who)
        if len(l_user) > 0:
            self.insertToDB(self.con, l_uwho_query, l_user)
        
        # res_inserts.append((self.make_query('h_whoprofile'), h_who))
        # res_inserts.append((self.make_query('s_whoprofile_schema'), s_who))
        # res_inserts.append((self.make_query('l_asset_whoprofile'), l_who))
        # res_inserts.append((self.make_query('l_whoprofileuser'), l_user))
        
        h_what = []
        s_what = []
        l_what = []
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_what_query = self.make_query('h_whatprofile')
        s_what_query = self.make_query('s_whatprofile_schema')
        l_what_query = self.make_query('l_asset_whatprofile')
        for row in self.csv_map['WhatProfile']:
            pid = row[0]
            ver = row[1]
            date = row[2]
            uid = row[3]
            aid = row[4]
            schema = row[5]
            h_what.append([pid, ver, date, uid])
            s_what.append([pid, pid, schema, ver, date, uid])
            self.keymap['L_Asset_WhatProfile'] += 1
            l_what.append([self.keymap['L_Asset_WhatProfile'],
                           aid, pid, ver, date, uid])
            
            h_chunksize += 1
            s_chunksize += 1
            l_chunksize += 1
            if h_chunksize >= 1000:
                h_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting h_Chunk Number: " + str(h_chunknum))
                self.insertToDB(self.con, h_what_query, h_what)
                h_what = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_what_query, s_what)
                s_what = []
                s_chunksize = 0
            if l_chunksize >= 1000:
                l_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(l_chunknum))
                self.insertToDB(self.con, l_what_query, l_what)
                l_what = []
                l_chunksize = 0
        
        #insert any leftovers:
        if len(h_what) > 0:
            self.insertToDB(self.con, h_what_query, h_what)
        if len(s_what) > 0:
            self.insertToDB(self.con, s_what_query, s_what)
        if len(l_what) > 0:
            self.insertToDB(self.con, l_what_query, l_what)
        
        # res_inserts.append((self.make_query('h_whatprofile'), h_what))
        # res_inserts.append((self.make_query('s_whatprofile_schema'), s_what))
        # res_inserts.append((self.make_query('l_asset_whatprofile'), l_what))
        
        h_how = []
        s_how = []
        l_how = []
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_how_query = self.make_query('h_howprofile')
        s_how_query = self.make_query('s_howprofile_schema')
        l_how_query = self.make_query('l_asset_howprofile')
        for row in self.csv_map['HowProfile']:
            pid = row[0]
            ver = row[1]
            date = row[2]
            uid = row[3]
            aid = row[4]
            schema = row[5]
            h_how.append([pid, ver, date, uid])
            s_how.append([pid, pid, schema, ver, date, uid])
            self.keymap['L_Asset_HowProfile'] += 1
            l_how.append([self.keymap['L_Asset_HowProfile'],
                          aid, pid, ver, date, uid])
            h_chunksize += 1
            s_chunksize += 1
            l_chunksize += 1
            if h_chunksize >= 1000:
                h_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting h_Chunk Number: " + str(h_chunknum))
                self.insertToDB(self.con, h_how_query, h_how)
                h_how = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_how_query, s_how)
                s_how = []
                s_chunksize = 0
            if l_chunksize >= 1000:
                l_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(l_chunknum))
                self.insertToDB(self.con, l_how_query, l_how)
                l_how = []
                l_chunksize = 0
        
        #insert any leftovers:
        if len(h_how) > 0:
            self.insertToDB(self.con, h_how_query, h_how)
        if len(s_how) > 0:
            self.insertToDB(self.con, s_how_query, s_how)
        if len(l_how) > 0:
            self.insertToDB(self.con, l_how_query, l_how)
        
        # res_inserts.append((self.make_query('h_howprofile'), h_how))
        # res_inserts.append((self.make_query('s_howprofile_schema'), s_how))
        # res_inserts.append((self.make_query('l_asset_howprofile'), l_how))
            
        h_why = []
        s_why = []
        l_why = []
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_why_query = self.make_query('h_whyprofile')
        s_why_query = self.make_query('s_whyprofile_schema')
        l_why_query = self.make_query('l_asset_whyprofile')
        for row in self.csv_map['WhyProfile']:
            pid = row[0]
            ver = row[1]
            date = row[2]
            uid = row[3]
            aid = row[4]
            schema = row[5]
            h_why.append([pid, ver, date, uid])
            s_why.append([pid, pid, schema, ver, date, uid])
            self.keymap['L_Asset_WhyProfile'] += 1
            l_why.append([self.keymap['L_Asset_WhyProfile'],
                          aid, pid, ver, date, uid])
            h_chunksize += 1
            s_chunksize += 1
            l_chunksize += 1
            if h_chunksize >= 1000:
                h_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting h_Chunk Number: " + str(h_chunknum))
                self.insertToDB(self.con, h_why_query, h_why)
                h_why = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_why_query, s_why)
                s_why = []
                s_chunksize = 0
            if l_chunksize >= 1000:
                l_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(l_chunknum))
                self.insertToDB(self.con, l_why_query, l_why)
                l_why = []
                l_chunksize = 0
        
        #insert any leftovers:
        if len(h_why) > 0:
            self.insertToDB(self.con, h_why_query, h_why)
        if len(s_why) > 0:
            self.insertToDB(self.con, s_why_query, s_why)
        if len(l_why) > 0:
            self.insertToDB(self.con, l_why_query, l_why)
        
        # res_inserts.append((self.make_query('h_whyprofile'), h_why))
        # res_inserts.append((self.make_query('s_whyprofile_schema'), s_why))
        # res_inserts.append((self.make_query('l_asset_whyprofile'), l_why))
        
        h_when = []
        s_when = []
        l_when = []
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_when_query = self.make_query('h_whenprofile')
        s_when_query = self.make_query('s_whenprofile_attributes')
        l_when_query = self.make_query('l_asset_whenprofile')
        for i,row in enumerate(self.csv_map['WhenProfile']):
            pid = row[0]
            ver = row[1]
            date = row[2]
            uid = row[3]
            aid = row[4]
            a_time = row[5]
            expiry = row[6]
            start = row[7]
            h_when.append([pid, ver, date, uid])
            s_when.append([pid, pid, a_time, expiry, start, ver, date, uid])
            self.keymap['L_Asset_WhenProfile'] += 1
            l_when.append([self.keymap['L_Asset_WhenProfile'],
                           aid, pid, ver, date, uid])
            h_chunksize += 1
            s_chunksize += 1
            l_chunksize += 1
            if h_chunksize >= 1000:
                h_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting h_Chunk Number: " + str(h_chunknum))
                self.insertToDB(self.con, h_when_query, h_when)
                h_when = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_when_query, s_when)
                s_when = []
                s_chunksize = 0
            if l_chunksize >= 1000:
                l_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(l_chunknum))
                self.insertToDB(self.con, l_when_query, l_when)
                l_when = []
                l_chunksize = 0
        
        #insert any leftovers:
        if len(h_when) > 0:
            self.insertToDB(self.con, h_when_query, h_when)
        if len(s_when) > 0:
            self.insertToDB(self.con, s_when_query, s_when)
        if len(l_when) > 0:
            self.insertToDB(self.con, l_when_query, l_when)
            
        # res_inserts.append((self.make_query('h_whenprofile'), h_when))
        # res_inserts.append((self.make_query('s_whenprofile_attributes'), s_when))
        # res_inserts.append((self.make_query('l_asset_whenprofile'), l_when))
        
        h_where = []
        s_where = []
        l_where = []
        l_2s = []
        h_chunksize = 0
        s_chunksize = 0
        la_chunksize = 0
        ls_chunksize = 0
        h_chunknum = 0
        la_chunknum = 0
        ls_chunknum = 0
        s_chunknum = 0
        h_where_query = self.make_query('h_whereprofile')
        s_where_query = self.make_query('s_configuration')
        la_where_query = self.make_query('l_asset_whereprofile')
        ls_where_query = self.make_query('l_whereprofile_source')
        for row in self.csv_map['WhereProfile']:
            pid = row[0]
            ver = row[1]
            date = row[2]
            uid = row[3]
            aid = row[4]
            access = row[5]
            sid = row[6]
            conf = row[7]
            h_where.append([pid, access, ver, date, uid])
            s_where.append([pid, conf, pid, ver, date, uid])
            self.keymap['L_Asset_WhereProfile'] += 1
            l_where.append([self.keymap['L_Asset_WhereProfile'],
                           aid, pid, ver, date, uid])
            self.keymap['L_WhereProfile_Source'] += 1
            l_2s.append([self.keymap['L_WhereProfile_Source'],
                         sid, pid, ver, date, uid])
            h_chunksize += 1
            s_chunksize += 1
            la_chunksize += 1
            ls_chunksize += 1
            if h_chunksize >= 1000:
                h_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting h_Chunk Number: " + str(h_chunknum))
                self.insertToDB(self.con, h_where_query, h_where)
                h_where = []
                h_chunksize = 0
            if s_chunksize >= 1000:
                s_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting s_Chunk Number: " + str(s_chunknum))
                self.insertToDB(self.con, s_where_query, s_where)
                s_where = []
                s_chunksize = 0
            if la_chunksize >= 1000:
                la_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(la_chunknum))
                self.insertToDB(self.con, la_where_query, l_where)
                l_where = []
                la_chunksize = 0
            if ls_chunksize >= 1000:
                ls_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(ls_chunknum))
                self.insertToDB(self.con, ls_where_query, l_2s)
                l_2s = []
                ls_chunksize = 0
            
        
        #insert any leftovers:
        if len(h_where) > 0:
            self.insertToDB(self.con, h_where_query, h_where)
        if len(s_where) > 0:
            self.insertToDB(self.con, s_where_query, s_where)
        if len(l_where) > 0:
            self.insertToDB(self.con, la_where_query, l_where)
        if len(l_2s) > 0:
            self.insertToDB(self.con, ls_where_query, l_2s)
            
        # res_inserts.append((self.make_query('h_whereprofile'), h_where))
        # res_inserts.append((self.make_query('s_configuration'), s_where))
        # res_inserts.append((self.make_query('l_asset_whereprofile'), l_where))
        # res_inserts.append((self.make_query('l_whereprofile_source'), l_2s))
        
        # return res_inserts
    
    def create_all_actions(self):
        h_actions = []
        l_actions = []
        h_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        h_action_query = self.make_query('h_action')
        l_action_query = self.make_query('l_assetsinactions')
        for i,row in enumerate(self.csv_map['Action']):
            pid = row[0]
            ver = row[1]
            date = row[2]
            uid = row[3]
            aid = row[4]
            who = row[5]
            how = row[6]
            why = row[7]
            when = row[8]
            h_actions.append([pid, ver, date, uid])
            self.keymap['L_AssetsInActions'] += 1
            l_actions.append([self.keymap['L_AssetsInActions'],
                              pid, aid, who, why, when, how, ver, date, uid])
            h_chunksize += 1
            l_chunksize += 1
            if h_chunksize >= 1000:
                h_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting h_Chunk Number: " + str(h_chunknum))
                self.insertToDB(self.con, h_action_query, h_actions)
                h_actions = []
                h_chunksize = 0
            if l_chunksize >= 1000:
                l_chunknum += 1
                print("Inserting through row: " + str(i))
                print("Inserting l_Chunk Number: " + str(l_chunknum))
                self.insertToDB(self.con, l_action_query, l_actions)
                l_actions = []
                l_chunksize = 0
        
        if len(h_actions) > 0:
            self.insertToDB(self.con, h_action_query, h_actions)
        if len(l_actions) > 0:
            self.insertToDB(self.con, l_action_query, l_actions)
            
        # res_inserts.append((self.make_query('h_action'), h_actions))
        # res_inserts.append((self.make_query('l_assetsinactions'), l_actions))
        # return res_inserts
    
    def run_full(self):
        self.con = sqlite3.connect('datavault_synthetic.db')
        cur = self.con.cursor()
        cur.execute("PRAGMA synchronous = OFF")
        cur.execute("PRAGMA journal_mode = MEMORY")
        cur.close()
        #self.create_type_records()
        #self.create_all_users()
        #self.create_all_assets()
        #self.create_all_sources()
        #self.create_all_relationships()
        self.create_records_per_table()
        self.create_all_actions()
        
        self.con.close()
    
    def insertToDB(self, con, query_str, queries):
        print("About to Execute: " + query_str)
        cur = con.cursor()
        cur.executemany(query_str, queries)
        con.commit()
        #close the cursor--maybe we're leaving too many of these around,
        #and that's slowing things down
        cur.close()
    
    def close_all(self):
        for key in self.fh_map:
            self.fh_map[key].close()
        

if __name__ == "__main__":
    record_writer = GenDSQLRecords()
    record_writer.run_full()
    record_writer.close_all()
        
        
            
