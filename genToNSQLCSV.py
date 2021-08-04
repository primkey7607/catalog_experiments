from backends.schema_models import *
import csv
import datetime
import random
import string
import sys
import sqlite3
from tqdm import tqdm

class GenNSQLCSV:
    
    def __init__(self, num_assets, num_records, rel_size, num_actions, num_sources, is_unique=False):
        self.num_assets = num_assets
        if is_unique:
            self.num_assets = 1
        self.num_records = num_records
        self.rel_size = rel_size
        self.num_actions = num_actions
        self.num_sources = num_sources
        self.asset_names = []
        self.ins_map = {}
        self.create_tables()
        self.keymap = {}
        self.init_keymap()
        self.csv_map = {}
        self.attrmap = {}
        self.init_attrmap()
        self.fh_map = {}
        self.create_csvs()
        self.common_props = {}
        self.init_props()
        self.version = 1
    
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
    
    def init_keymap(self):
        self.keymap['UserType'] = 1
        self.keymap['User'] = 1
        self.keymap['AssetType'] = 0
        self.keymap['Asset'] = 0
        self.keymap['WhoProfile'] = 0
        self.keymap['WhatProfile'] = 0
        self.keymap['HowProfile'] = 0
        self.keymap['WhyProfile'] = 0
        self.keymap['WhenProfile'] = 0
        self.keymap['SourceType'] = 0
        self.keymap['Source'] = 0
        self.keymap['WhereProfile'] = 0
        self.keymap['RelationshipType'] = 0
        self.keymap['Relationship'] = 0
        self.keymap['Asset_Relationships'] = 0
        self.keymap['Action'] = 0
    
    def init_attrmap(self):
        self.attrmap['UserType'] = ['name', 'description']
        self.attrmap['User'] = ['name', 'user_type', 'schema',
                                'version', 'timestamp', 'user']
        self.attrmap['AssetType'] = ['name', 'description']
        self.attrmap['Asset'] = ['name', 'asset_type', 'version',
                                 'timestamp', 'user']
        self.attrmap['WhoProfile'] = ['version', 'timestamp', 'write_user',
                                      'asset', 'user', 'schema']
        self.attrmap['WhatProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'schema']
        self.attrmap['HowProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'schema']
        self.attrmap['WhyProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'schema']
        self.attrmap['WhenProfile'] = ['version', 'timestamp', 'user',
                                      'asset', 'asset_timestamp',
                                      'expiry_date', 'start_date']
        self.attrmap['SourceType'] = ['connector', 'serde', 'datamodel']
        self.attrmap['Source'] = ['version', 'timestamp', 'user',
                                  'name', 'source_type', 'schema']
        self.attrmap['WhereProfile'] = ['version', 'timestamp', 'user',
                                        'asset', 'access_path', 'source', 'configuration']
        self.attrmap['RelationshipType'] = ['name', 'description']
        self.attrmap['Relationship'] = ['version', 'timestamp', 'user',
                                        'relationship_type', 'schema']
        self.attrmap['Asset_Relationships'] = ['asset', 'relationship']
        self.attrmap['Action'] = ['version', 'timestamp', 'user', 'timestamp',
                                  'asset', 'who', 'how', 'why', 'when']
    
    def create_tables(self):
        database.init('normalized_synthetic.db')
        with database:
            #add following to below if needed later: Item,
            database.create_tables([UserType, User, AssetType,
                                    Asset, WhoProfile, WhatProfile,
                                    HowProfile, WhyProfile, WhenProfile,
                                    SourceType, Source, WhereProfile, 
                                    Action, RelationshipType, Relationship,
                                    Asset_Relationships])
            # Item.__schema.create_foreign_key(Item.user)
            #self.ins_map["Item"] = Item
            self.ins_map["UserType"] = UserType
            self.ins_map["User"] = User
            self.ins_map["AssetType"] = AssetType
            self.ins_map["Asset"] = Asset
            self.ins_map["WhoProfile"] = WhoProfile
            self.ins_map["WhatProfile"] = WhatProfile
            self.ins_map["HowProfile"] = HowProfile
            self.ins_map["WhyProfile"] = WhyProfile
            self.ins_map["WhenProfile"] = WhenProfile
            self.ins_map["SourceType"] = SourceType
            self.ins_map["Source"] = Source
            self.ins_map["WhereProfile"] = WhereProfile
            self.ins_map["Action"] = Action
            self.ins_map["RelationshipType"] = RelationshipType
            self.ins_map["Relationship"] = Relationship
            self.ins_map['Asset_Relationships'] = Asset_Relationships
            
            UserType.insert({"name" : "administrator", "description" : "responsible for populating the catalog"}).execute()
            User.insert({"name": "admin", "user_type": 1, 
                         "version": 1, "timestamp" : str(datetime.datetime.now()),
                         "schema": {"addr":{"home":"westlake","company":"bank"},"phone":1234567}}).execute()
    
    def create_csvs(self):
        for key in self.attrmap:
            fh = open(key + '.csv', 'w+')
            
            csvwriter = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            self.fh_map[key] = fh
            self.csv_map[key] = csvwriter
            #self.csv_map[key].writeheader()
    
    def random_date(self):
        start = datetime.datetime.strptime('6/10/2020 12:00 AM', '%m/%d/%Y %I:%M %p')
        end = datetime.datetime.now()
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = random.randrange(int_delta)
        return start + datetime.timedelta(seconds=random_second)
    
    #do this first, before creating records for other tables
    def create_type_records(self):
        #do UserType first
        for p in self.common_props['UserType']:
            desc = ''.join(random.choices(string.ascii_letters, k=50))
            self.keymap['UserType'] += 1
            self.csv_map['UserType'].writerow([self.keymap['UserType'],
                                              p, desc])
            
        for p in self.common_props['AssetType']:
            self.keymap['AssetType'] += 1
            desc = ''.join(random.choices(string.ascii_letters, k=50))
            self.csv_map['AssetType'].writerow([self.keymap['AssetType'],
                                               p, desc])
        
        for p in self.common_props['RelationshipType']:
            self.keymap['RelationshipType'] += 1
            desc = ''.join(random.choices(string.ascii_letters, k=50))
            self.csv_map['RelationshipType'].writerow([self.keymap['RelationshipType'],
                                               p, desc])
        
        for c in self.common_props['SourceType_connector']:
            for s in self.common_props['SourceType_serde']:
                for d in self.common_props['SourceType_datamodel']:
                    self.keymap['SourceType'] += 1
                    self.csv_map['SourceType'].writerow([self.keymap['SourceType'],
                                                        c, s, d])
        
        
    def create_user(self):
        #First User
        self.keymap['User'] += 1
        user_pid = self.keymap['User']
        name = ''.join(random.choices(string.ascii_letters, k=12))
        user_type_id = random.choice(range(1, self.keymap['UserType']+1))
        schema_key = random.choice(self.common_props['User'])
        schema_value = ''.join(random.choices(string.ascii_letters, k = 25))
        schema = '"{ ' + schema_key + ' : ' + schema_value + '}"'
        user_id = 1 #assume one user is responsible for entering in
        #all other users
        self.csv_map['User'].writerow([user_pid,
                                      name,
                                      user_type_id,
                                      schema, self.version,
                                      str(self.random_date()),
                                      user_id])
    
    def create_all_users(self):
        for i in range(self.num_records):
            self.create_user()
    
    def create_all_assets(self):
        for i in range(self.num_assets):
            self.keymap['Asset'] += 1
            asset_id = self.keymap['Asset']
            name = ''.join(random.choices(string.ascii_letters, k=12))
            self.asset_names.append(name)
            asset_type_id = random.choice(range(1, self.keymap['AssetType']+1))
            user_id = random.choice(range(1, self.keymap['User']+1))
            self.csv_map['Asset'].writerow([asset_id, name,
                                           asset_type_id,
                                           self.version,
                                           self.random_date(),
                                           user_id])
    
    def create_all_sources(self):
        for i in range(self.num_sources):
            self.keymap['Source'] += 1
            user_id = random.choice(range(1, self.keymap['User']+1))
            name = ''.join(random.choices(string.ascii_letters, k=25))
            source_type_id = random.choice(range(1, self.keymap['SourceType']+1))
            schema_key = random.choice(self.common_props['Source'])
            schema_value = ''.join(random.choices(string.ascii_letters, k=12))
            schema = '"{ ' + schema_key + ' : ' + schema_value + '}"'
            self.csv_map['Source'].writerow([self.keymap['Source'],
                                             self.version, self.random_date(),
                                             user_id, name, source_type_id,
                                             schema])
    
    def create_all_relationships(self):
        actual_size = -1
        if (self.rel_size > self.num_assets):
            actual_size = self.num_assets
        else:
            actual_size = self.rel_size
        
        enm = enumerate(self.asset_names)
        enmlst = list(enm)
        print("Actual Size of Relationships: " + str(actual_size))
        for i in range(1, self.keymap['Asset']+1):
            rel_names = random.sample(enmlst, actual_size)
            self.keymap['Relationship'] += 1
            rel_type_id = random.choice(range(1, self.keymap['RelationshipType']+1))
            schema_key = random.choice(self.common_props['Relationship'])
            schema_value = '_'.join([r[1] for r in rel_names])
            schema = '"{ ' + schema_key + ' : ' + schema_value + '}"'
            user_id = random.choice(range(1, self.keymap['User']+1))
            self.csv_map['Relationship'].writerow([self.keymap['Relationship'],
                                                   self.version,
                                                   self.random_date(),
                                                   user_id, rel_type_id,
                                                   schema])
            
            
            for r in rel_names:
                a_id = r[0] + 1
                self.keymap['Asset_Relationships'] += 1
                self.csv_map['Asset_Relationships'].writerow([self.keymap['Asset_Relationships'], a_id,
                                       self.keymap['Relationship']])
    
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
    
    def create_records_per_table(self):
        for i in range(self.num_records):
            
            #WhoProfile
            self.keymap['WhoProfile'] += 1
            write_user_id = random.choice(range(1, self.keymap['User']+1))
            user_id = random.choice(range(1, self.keymap['User']+1))
            asset_id = random.choice(range(1, self.keymap['Asset']+1))
            schema_key = random.choice(self.common_props['WhoProfile'])
            schema_value = ''.join(random.choices(string.ascii_letters, k=25))
            schema = '"{ ' + schema_key + ' : ' + schema_value + ' }"'
            self.csv_map['WhoProfile'].writerow([self.keymap['WhoProfile'],
                                                 self.version,
                                                 self.random_date(),
                                                 write_user_id,
                                                 asset_id, user_id,
                                                 schema])
            
            #WhatProfile
            self.keymap['WhatProfile'] += 1
            asset_id = random.choice(range(1, self.keymap['Asset']+1))
            user_id = random.choice(range(1, self.keymap['User']+1))
            #first, randomly choose whether to generate numerical, or text
            is_num = random.choice([0,1])
            if is_num == 1:
                schema_key = random.choice(self.common_props['WhatProfile_num'])
                schema_value = str(random.uniform(1.0, 100000.0))
                
            else:
                schema_key = random.choice(self.common_props['WhatProfile_text'])
                schema_value = ''.join(random.choices(string.ascii_letters, k=25))
            
            schema = '"{ ' + schema_key + ' : ' + schema_value + ' }"'
            self.csv_map['WhatProfile'].writerow([self.keymap['WhatProfile'],
                                                  self.version,
                                                  self.random_date(),
                                                  user_id, asset_id, schema])
            
            #HowProfile
            self.keymap['HowProfile'] += 1
            asset_id = random.choice(range(1, self.keymap['Asset']+1))
            user_id = random.choice(range(1, self.keymap['User']+1))
            schema_key = random.choice(self.common_props['HowProfile'])
            schema_value = ''.join(random.choices(string.ascii_letters, k=25))
            schema = '"{ ' + schema_key + ' : ' + schema_value + ' }"'
            self.csv_map['HowProfile'].writerow([self.keymap['HowProfile'],
                                                  self.version,
                                                  self.random_date(),
                                                  user_id, asset_id, schema])
            
            #WhyProfile
            self.keymap['WhyProfile'] += 1
            asset_id = random.choice(range(1, self.keymap['Asset']+1))
            user_id = random.choice(range(1, self.keymap['User']+1))
            schema_key = random.choice(self.common_props['WhyProfile'])
            schema_value = ''.join(random.choices(string.ascii_letters, k=25))
            schema = '"{ ' + schema_key + ' : ' + schema_value + ' }"'
            self.csv_map['WhyProfile'].writerow([self.keymap['WhyProfile'],
                                                  self.version,
                                                  self.random_date(),
                                                  user_id, asset_id, schema])
            
            #WhenProfile
            self.keymap['WhenProfile'] += 1
            asset_id = random.choice(range(1, self.keymap['Asset']+1))
            user_id = random.choice(range(1, self.keymap['User']+1))
            start = datetime.datetime.strptime('6/1/2020 12:00 AM', '%m/%d/%Y %I:%M %p')
            end = self.random_date()
            startlst = self.k_in_time(start, end, 2)
            start_date = startlst[0]
            asset_timestamp = startlst[1]
            start2 = datetime.datetime.strptime('11/28/2020 12:00 AM', '%m/%d/%Y %I:%M %p')
            end2 = datetime.datetime.strptime('1/15/2021 12:00 AM', '%m/%d/%Y %I:%M %p')
            endlst = self.k_in_time(start2, end2, 1)
            expiry_date = endlst[0]
            self.csv_map['WhenProfile'].writerow([self.keymap['WhenProfile'],
                                                  self.version,
                                                  end, user_id, asset_id,
                                                  asset_timestamp,
                                                  expiry_date, start_date])
            
            #WhereProfile
            self.keymap['WhereProfile'] += 1
            asset_id = random.choice(range(1, self.keymap['Asset']+1))
            user_id = random.choice(range(1, self.keymap['User']+1))
            access_path = ''.join(random.choices(string.ascii_letters, k=25))
            config_key = random.choice(self.common_props['WhereProfile_conf'])
            config_value = ''.join(random.choices(string.ascii_letters, k=20))
            config = '"{ ' + config_key + ' : ' + config_value + ' }"'
            source_id = random.choice(range(1, self.keymap['Source']))
            self.csv_map['WhereProfile'].writerow([self.keymap['WhereProfile'],
                                                  self.version,
                                                  self.random_date(),
                                                  user_id,
                                                  asset_id, access_path,
                                                  source_id, config])
            
    def create_all_actions(self):
        for i in range(self.num_actions):
            self.keymap['Action'] += 1
            asset_id = random.choice(range(1, self.keymap['Asset']+1))
            who_id = random.choice(range(1, self.keymap['WhoProfile']+1))
            how_id = random.choice(range(1, self.keymap['HowProfile']+1))
            why_id = random.choice(range(1, self.keymap['WhyProfile']+1))
            when_id = random.choice(range(1, self.keymap['WhenProfile']+1))
            user_id = random.choice(range(1, self.keymap['User']+1))
            self.csv_map['Action'].writerow([self.keymap['Action'],
                                             self.version,
                                             self.random_date(),
                                             user_id, asset_id,
                                             who_id, how_id, why_id, when_id])
    
    def run_full(self):
        self.create_type_records()
        self.create_all_users()
        self.create_all_assets()
        self.create_all_sources()
        self.create_all_relationships()
        self.create_records_per_table()
        self.create_all_actions()
        
        
    
    def close_all(self):
        for key in self.fh_map:
            self.fh_map[key].close()
    
    def make_query(self, tname):
        query = 'INSERT INTO ' + tname.lower() + ' VALUES ('
        con = sqlite3.connect('normalized_synthetic.db')
        cur = con.cursor()
        schema = cur.execute("PRAGMA table_info('" + tname + "');").fetchall()
        con.close()
        rowlen = len(schema)
        for i in range(rowlen):
            query += '?,'
        query = query[:-1] + ');'
        return query
    
    def insertToDB(self, con, query_str, queries):
        print("About to Execute: " + query_str)
        cur = con.cursor()
        cur.executemany(query_str, queries)
        con.commit()
        #close the cursor--maybe we're leaving too many of these around,
        #and that's slowing things down
        cur.close()
    
    def perform_inserts(self):
        #first, it looks like we have some big fields, so...
        csv.field_size_limit(int(sys.maxsize/10))
        con = sqlite3.connect('normalized_synthetic.db')
        cur = con.cursor()
        cur.execute("PRAGMA synchronous = OFF")
        cur.execute("PRAGMA journal_mode = MEMORY")
        cur.close()
        for key in tqdm(self.ins_map):
            inserts = []
            rowlen = -1
            numrows = 0
            chunksize = 0
            chunknum = 0
            query_str = self.make_query(key)
            with open(key + '.csv', 'r') as fh:
                csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for i,row in enumerate(csvreader):
                    rowlen = len(row)
                    numrows += 1
                    if rowlen == 0:
                        continue
                    inserts.append(row)
                    chunksize += 1
                    if chunksize >= 1000:
                        chunknum += 1
                        print("Inserting through row: " + str(i))
                        print("Inserting Chunk Number: " + str(chunknum))
                        self.insertToDB(con, query_str, inserts)
                        inserts = []
                        chunksize = 0
                
            if len(inserts) > 0:
                self.insertToDB(con, query_str, inserts)
                inserts = []
        con.close()


if __name__ == "__main__":
    csv_creator = GenNSQLCSV(10000,10000,5,10000,10000)
    #The above generates a 0.014 GB normalized database
    #And a 0.029 GB datavault database
    #csv_creator = GenNSQLCSV(100000,100000,5,10000,10000)
    #The above generates 0.129978368 GB normalized
    #0.26914816 GB datavault
    #The below generates a 13.23 GB normalized DB,
    #and a 27.93 GB datavault DB
    #And the dataset itself is 9.42 GB
    #csv_creator = GenNSQLCSV(10000000,10000000,5,10000,10000)
    #So, to generate a 50GB dataset, we need roughly 5.3 times the 
    #number of records we have here. But our databases are going to be much larger
    #than 50GB
    #csv_creator = GenNSQLCSV(53000000,53000000,5,10000,10000)
    csv_creator.run_full()
    csv_creator.close_all()
    #Since performing the inserts has a problem, we won't do that for now
    #we'll just generate the CSVs, and then manually import the files
    #csv_creator.perform_inserts()
