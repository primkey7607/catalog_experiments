from neo4j import GraphDatabase
import os
import csv
import datetime
from neo4j.time import DateTime
import sys
import time
import cProfile, pstats
import io

class NNeo4j_Queries:
    
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.attrmap = {}
        self.init_attrmap()
    
    def init_attrmap(self):
        self.attrmap['UserType'] = ['id', 'name', 'description']
        self.attrmap['User'] = ['id', 'name', 'user_type_id', 'schema',
                                'version', 'timestamp', 'user_id']
        self.attrmap['AssetType'] = ['id', 'name', 'description']
        self.attrmap['Asset'] = ['id', 'name', 'asset_type_id', 'version',
                                 'timestamp', 'user_id']
        self.attrmap['WhoProfile'] = ['id', 'version', 'timestamp', 'write_user_id',
                                      'asset_id', 'user_id', 'schema']
        self.attrmap['WhatProfile'] = ['id', 'version', 'timestamp', 'user_id',
                                      'asset_id', 'schema']
        self.attrmap['HowProfile'] = ['id', 'version', 'timestamp', 'user_id',
                                      'asset_id', 'schema']
        self.attrmap['WhyProfile'] = ['id', 'version', 'timestamp', 'user_id',
                                      'asset_id', 'schema']
        self.attrmap['WhenProfile'] = ['id', 'version', 'timestamp', 'user_id',
                                      'asset_id', 'asset_timestamp',
                                      'expiry_date', 'start_date']
        self.attrmap['SourceType'] = ['id', 'connector', 'serde', 'datamodel']
        self.attrmap['Source'] = ['id', 'version', 'timestamp', 'user_id',
                                  'name', 'source_type_id', 'schema']
        self.attrmap['WhereProfile'] = ['id', 'version', 'timestamp', 'user_id',
                                        'asset_id', 'access_path', 'source_id', 'configuration']
        self.attrmap['RelationshipType'] = ['id', 'name', 'description']
        self.attrmap['Relationship'] = ['id', 'version', 'timestamp', 'user_id',
                                        'relationship_type_id', 'schema']
        self.attrmap['Asset_Relationships'] = ['id', 'asset_id', 'relationship_id']
        self.attrmap['Action'] = ['id', 'version', 'timestamp', 'user_id',
                                  'asset_id', 'who_id', 'how_id', 'why_id',
                                  'when_id']
    
    def getDataType(self, attrName):
        if 'id' in attrName:
            return 'int'
        elif attrName == 'version':
            return 'int'
        elif 'timestamp' in attrName or 'date' in attrName:
            return 'datetime'
        else:
            return 'string'
    
    def make_q1_query(self, tname):
        query_str1 = "UNWIND $props AS map CREATE (n:" + tname + ") SET n = map"
        query_str1 += " RETURN n"
        query_str2 = "MATCH (m:User) WHERE m.id = n.user_id CREATE (n)-[:Rel_WhatProfile_User]->(m)"
        
        query_str = 'call apoc.periodic.iterate("' + query_str1 + '", '
        query_str += '"' + query_str2 + '", {batchSize:1000}) YIELD batches, total, errorMessages return batches, total, errorMessages'
        
        return query_str
    
    def get_lastId(self, tname):
        query_str = "MATCH (n:" + tname + ") RETURN max(n.id);"
        with self.driver.session() as session:
            result = session.run(query_str)
            return result.single()[0]
    
    def create_relquery(self, t1, t2, k1, k2):
        #sample: MATCH (a:Person),(b:Person)
        #WHERE a.name = 'A' AND b.name = 'B'
        #CREATE (a)-[r:RELTYPE]->(b)
        #RETURN type(r)
        #query_str = 'call apoc.periodic.iterate("match(u:' + t1 + ') with u return u", "match (b:' + t2 + ') where u.' + k1 + ' = b.' + k2 + ' create (u)-[r:RELTYPE]->(b)", {batchSize:1000}) YIELD batches, total, errorMessages return batches, total, errorMessages'
        query_str = "MATCH (a:" + t1 + ") MATCH (b:" + t2 + ") "
        #we no longer need this
        #query_str += "WHERE a." + k1 + " = b." + k2
        rel_name = 'Rel_' + t1 + '_' + t2
        query_str += " MERGE (a)-[r:" + rel_name + "]->(b);"
        return query_str
    
    def create_q2_relquery(self, t1, t2, k1, k2, key):
        #sample: MATCH (a:Person),(b:Person)
        #WHERE a.name = 'A' AND b.name = 'B'
        #CREATE (a)-[r:RELTYPE]->(b)
        #RETURN type(r)
        #query_str = 'call apoc.periodic.iterate("match(u:' + t1 + ') with u return u", "match (b:' + t2 + ') where u.' + k1 + ' = b.' + k2 + ' create (u)-[r:RELTYPE]->(b)", {batchSize:1000}) YIELD batches, total, errorMessages return batches, total, errorMessages'
        query_str = "MATCH (a:" + t1 + ") MATCH (b:" + t2 + ") "
        query_str += "WHERE a." + k1 + " = " + str(key) + " AND b." + k2
        rel_name = 'Rel_' + t1 + '_' + t2
        query_str += " = " + str(key) + " MERGE (a)-[r:" + rel_name + "]->(b);"
        return query_str
    
    def insertToDB(self, tname, query_str, recs, qnum, rel_queries):
        recmap = []
        attrs = self.attrmap[tname]
        for r in recs:
            tmpdict = {}
            for i,a in enumerate(attrs):
                dtype = self.getDataType(str(a))
                if dtype == 'int':
                    tmpdict[a] = int(r[i])
                elif dtype == 'datetime':
                    #dt = datetime.datetime.strptime(r[i], '%Y-%m-%d %H:%M:%S')
                    dt = r[i]
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    tmpdict[a] = nd
                else:
                    tmpdict[a] = r[i]
            recmap.append(tmpdict)
        
        print("Executing query " + str(qnum) + ": " + query_str)
        
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str, props=recmap)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q' + str(qnum) + '_nneo4j_test.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q1(self):
        tname = 'WhatProfile'
        dname = '/home/pranav/catalog_experiments/' + tname
        numrows = 0
        last_id = self.get_lastId(tname)
        inserts = []
        with open(dname + '.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in csvreader:
                if numrows >= 100000:
                    break
                rowlen = len(row)
                numrows += 1
                if rowlen == 0:
                    continue
                newrow = []
                for i,c in enumerate(row):
                    if i == 0:
                        newrow.append(str(last_id + numrows))
                    elif i == 1:
                        newrow.append(str(int(c) + 1))
                    elif i == 2:
                        #newrow.append(str(datetime.datetime.now()))
                        newrow.append(datetime.datetime.now())
                    else:
                        newrow.append(c)
                    
                inserts.append(newrow)
        #construct the query
        if numrows == 0:
            return
        query = self.make_q1_query(tname)
        rel_query1 = self.create_relquery('WhatProfile', 'User', 'user_id', 'id')
        rel_query2 = self.create_relquery('WhatProfile', 'Asset', 'asset_id', 'id')
        rel_queries = []
        rel_queries.append(rel_query1)
        rel_queries.append(rel_query2)
        self.insertToDB(tname, query, inserts, 1, rel_queries)
    
    def execute_q2(self):
        asset_id = self.get_lastId('Asset')
        profid = self.get_lastId('HowProfile')
        schema = '\"{ denoisingProcedure : run denoise.py with normalize set to true }\" '
        version = 1
        uid = 1
        dt = datetime.datetime.now()
        nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
        # query_str = 'INSERT INTO howprofile VALUES ('
        
        # query_str += str(profid + 1) + ', ' + str(version) + ', ' + '\"' + str(datetime.datetime.now()) + '\"' + ', '
        # query_str += str(uid) + ', ' + str(asset_id) + ', ' + str(schema) + ');'
        query_str = 'MERGE (n:HowProfile { id: ' + str(profid + 1)
        query_str += ' version: ' + str(version) + ' timestamp: $nd'
        query_str += ' user_id: ' + str(uid) + ' asset_id: ' + str(asset_id)
        query_str += ' schema: ' + str(schema) + '})'
        
        
        print("Executing Query 2: " + query_str)
        rel_query1 = self.create_q2_relquery('HowProfile', 'User', 'user_id', 'id', uid)
        rel_query2 = self.create_q2_relquery('HowProfile', 'Asset', 'asset_id', 'id', asset_id)
        print("Executing Query 2 Relationships: " + rel_query1)
        print(rel_query2)
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str, nd=nd)
            session.run(rel_query1)
            session.run(rel_query2)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q2_nneo4j_test.txt', 'w+') as f:
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
        for r in recs:
            tmpdict = {}
            for i,a in enumerate(attrs):
                dtype = self.getDataType(a)
                if dtype == 'int' and 'asset_id' in a:
                    tmpdict[a] = asset_id
                elif dtype == 'int' and a == 'id':
                    tmpdict[a] = tid + i + 1
                elif dtype == 'int':
                    tmpdict[a] = int(r[i])
                elif dtype == 'datetime':
                    dt = datetime.datetime.strptime(r[i], '%Y-%m-%d %H:%M:%S')
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    tmpdict[a] = nd
                else:
                    tmpdict[a] = r[i]
            recmap.append(tmpdict)
        
        return recmap
        
        
    
    def execute_q3(self, x):
        #these are the ids of the last records
        why_name = 'WhyProfile'
        who_name = 'WhoProfile'
        how_name = 'HowProfile'
        when_name = 'WhenProfile'
        action_name = 'Action'
        who_id = self.get_lastId(who_name)
        how_id = self.get_lastId(how_name)
        when_id = self.get_lastId(when_name)
        why_id = self.get_lastId(why_name)
        action_id = self.get_lastId(action_name)
        #create a fictional asset that all these new profiles are going to be about
        common_asset = self.get_lastId('Asset') + 1
        
        who_inserts = self.get_q3inserts(who_name, who_id, common_asset, 3, x)
        how_inserts = self.get_q3inserts(how_name, how_id, common_asset, 3, x)
        why_inserts = self.get_q3inserts(why_name, why_id, common_asset, 3, x)
        when_inserts = self.get_q3inserts(when_name, when_id, common_asset, 3, x)
        
        adict = {}
        with open('Asset.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            firstrow = []
            for i,row in csvreader:
                if i > 0:
                    break
                else:
                    firstrow = row
            
            
            for i,a in enumerate(self.attrmap['Asset']):
                dtype = self.getDataType(a)
                if dtype == 'int' and a == 'id':
                    adict[a] = common_asset
                elif dtype == 'int':
                    adict[a] = int(firstrow[i])
                elif dtype == 'datetime':
                    dt = datetime.datetime.strptime(firstrow[i], '%Y-%m-%d %H:%M:%S')
                    nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
                    adict[a] = nd
                else:
                    adict[a] = firstrow[i]
        
        asset_query = 'CREATE (n:Asset) SET n = $dct'
        with self.driver.session() as session:
            session.run(asset_query, dct=adict)
        
        who_query = self.make_q1_query(who_name)
        who_relquery1 = self.create_relquery('WhoProfile', 'User', 'write_user_id', 'id')
        who_relquery2 = self.create_relquery('WhoProfile', 'User', 'user_id', 'id')
        who_relquery3 = self.create_relquery('WhoProfile', 'Asset', 'asset_id', 'id')
        how_query = self.make_q1_query(how_name)
        how_relquery1 = self.create_relquery('HowProfile', 'User', 'user_id', 'id')
        how_relquery2 = self.create_relquery('HowProfile', 'Asset', 'asset_id', 'id')
        why_query = self.make_q1_query(why_name)
        why_relquery1 = self.create_relquery('WhyProfile', 'User', 'user_id', 'id')
        why_relquery2 = self.create_relquery('WhyProfile', 'Asset', 'asset_id', 'id')
        when_query = self.make_q1_query(when_name)
        when_relquery1 = self.create_relquery('WhenProfile', 'User', 'user_id', 'id')
        when_relquery2 = self.create_relquery('WhenProfile', 'Asset', 'asset_id', 'id')
        
        #create the action inserts and queries now
        action_inserts = []
        action_query = self.make_q1_query('Action')
        for i in range(x):
            #['id', 'version', 'timestamp', 'user_id',
            #'asset_id', 'who_id', 'how_id', 'why_id',
            #'when_id']
            actdict = {}
            actdict['id'] = action_id + 1 + i
            actdict['version'] = 1
            dt = datetime.datetime.now()
            nd = DateTime(dt.year, dt.month, dt.day, dt.hour, dt.minute, float(dt.second))
            actdict['timestamp'] = nd
            actdict['user_id'] = 1
            actdict['asset_id'] = common_asset
            actdict['who_id'] = who_inserts[i]['id']
            actdict['how_id'] = how_inserts[i]['id']
            actdict['why_id'] = why_inserts[i]['id']
            actdict['when_id'] = when_inserts[i]['id']
            action_inserts.append(actdict)
        
        action_relquery1 = self.create_relquery('Action', 'User', 'user_id', 'id')
        action_relquery2 = self.create_relquery('Action', 'Asset', 'asset_id', 'id')
        action_relquery3 = self.create_relquery('Action', 'WhoProfile', 'who_id', 'id')
        action_relquery4 = self.create_relquery('Action', 'WhoProfile', 'who_id', 'id')
        action_relquery5 = self.create_relquery('Action', 'HowProfile', 'how_id', 'id')
        action_relquery6 = self.create_relquery('Action', 'WhyProfile', 'why_id', 'id')
        action_relquery7 = self.create_relquery('Action', 'WhenProfile', 'when_id', 'id')
        
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(who_query, props=who_inserts)
            session.run(who_relquery1)
            session.run(who_relquery2)
            session.run(who_relquery3)
            session.run(how_query, props=how_inserts)
            session.run(how_relquery1)
            session.run(how_relquery2)
            session.run(why_query, props=why_inserts)
            session.run(why_relquery1)
            session.run(why_relquery2)
            session.run(when_query, props=when_inserts)
            session.run(when_relquery1)
            session.run(when_relquery2)
            session.run(action_query, props=action_inserts)
            session.run(action_relquery1)
            session.run(action_relquery2)
            session.run(action_relquery3)
            session.run(action_relquery4)
            session.run(action_relquery5)
            session.run(action_relquery6)
            session.run(action_relquery7)
        
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q3_nneo4j_test.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q4(self, trial):
        query_str = 'WITH datetime({year: 2020, month: 12, day: 20}).epochMillis AS d1 '
        query_str = 'WITH datetime({year: 2020, month: 11, day: 01}).epochMillis as d2 '
        query_str += 'MATCH (n:WhatProfile) WHERE n.timestamp >= d1 AND n.timestamp <= d2;'
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q4_nneo4j_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q5(self, trial):
        query_str = 'MATCH (ac:Action)-[:Rel_Action_WhoProfile]->(who:WhoProfile)-[:Rel_WhoProfile_User]->(u:User) '
        query_str += 'MATCH (ac)-[:Rel_Action_WhyProfile]->(why:WhyProfile) '
        query_str += 'MATCH (ac)-[:Rel_Action_WhenProfile]->(when:WhenProfile) '
        query_str += 'MATCH (ac)-[:Rel_Action_HowProfile]->(how:HowProfile) '
        query_str += 'RETURN u.name, how.schema, when.asset_timestamp, why.schema;'
        
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q5_nneo4j_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q6(self, trial):
        
        query_str = 'MATCH (ac:Action)-[:Rel_Action_WhoProfile]->(who:WhoProfile)-[:Rel_WhoProfile_User]->(u:User) '
        query_str += 'MATCH (ac)-[:Rel_Action_WhyProfile]->(why:WhyProfile) '
        query_str += 'MATCH (ac)-[:Rel_Action_WhenProfile]->(when:WhenProfile) '
        query_str += 'MATCH (ac)-[:Rel_Action_HowProfile]->(how:HowProfile) '
        query_str += 'RETURN who.schema, how.schema, when.asset_timestamp, why.schema '
        query_str += 'ORDER BY ac.timestamp DESC LIMIT 10;'
        
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q6_nneo4j_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q7(self, trial):
        query_str = 'MATCH (h:HowProfile)-[r:Rel_HowProfile_Asset]->(a:Asset) '
        query_str += 'RETURN a.name, count(*);'
    
        pr = cProfile.Profile()
        pr.enable()
        with self.driver.session() as session:
            session.run(query_str)
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q7_nneo4j_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_full(self):
        self.execute_q1()
        self.execute_q2()
        self.execute_q3(100000)
        #repeat the other experiments 5 times
        for i in range(6):
            #we get 2407903 records from executing q4
            #drop the cache before executing anything
            os.system('echo 1 | sudo tee /proc/sys/vm/drop_caches')
            time.sleep(5)
            self.execute_q4(i)
            self.execute_q5(i)
            self.execute_q6(i)
            self.execute_q7(i)
        
    def close(self):
        self.driver.close()

if __name__ == "__main__":
    neo_queries = NNeo4j_Queries("bolt://localhost:7687", "neo4j", "normal")
    neo_queries.execute_full()


