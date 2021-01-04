import csv
import datetime
import sqlite3
import sys
import cProfile, pstats
import io

class NSQL_Queries:
    
    def __init__(self, dbfile, resfile):
        csv.field_size_limit(int(sys.maxsize/10))
        self.con = sqlite3.connect(dbfile)
        #make sure the sqlite database is enforcing foreign key constraints
        cur = self.con.cursor()
        cur.execute('PRAGMA foreign_keys = ON;')
        cur.close()
        self.resfile = resfile
    
    def create_out(self, resfile):
        fh = open(resfile, 'w+')
        initwrite = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        initwrite.writerow(['Query Number', 'Query String', 'Runtime', 'Queries per Sec'])
        fh.close()
    
    def close_conn(self):
        self.con.close()
    
    def get_lastId(self, tname):
        query_str = "select max(id) from " + tname.lower() + ";"
        cur = self.con.cursor()
        maxlst = cur.execute(query_str).fetchone()
        #we're going to want to see what this is
        print(maxlst)
        return maxlst[0]
    
    def get_inserts(self, dname, tname, tsize):
        rowlen = 0
        numrows = 0
        res_inserts = []
        cur = self.con.cursor()
        schema = cur.execute("PRAGMA table_info('" + tname + "')").fetchall()
        vertup = [i for i in schema if 'version' in i[1]]
        ver_ind = vertup[0][0]
        print(ver_ind)
        timetup = [i for i in schema if 'timestamp' in i[1]]
        time_ind = timetup[0][0]
        print(time_ind)
        
        with open(dname + '.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in csvreader:
                if numrows >= 10000:
                    break
                rowlen = len(row)
                numrows += 1
                if rowlen == 0:
                    continue
                newrow = []
                for i,c in enumerate(row):
                    if i == 0:
                        newrow.append(str(tsize + numrows))
                    elif i == ver_ind: #update version
                        newrow.append(str(int(c) + 1))
                    elif i == time_ind: #update timestamp
                        newrow.append(str(datetime.datetime.now()))
                    else:
                        newrow.append(c)
                    
                res_inserts.append(newrow)
        
        if numrows == 0:
            return None
        
        query_str = 'INSERT INTO ' + tname.lower() + ' VALUES ('
        for i in range(rowlen):
            query_str += '?,'
        query_str = query_str[:-1] + ');'
        
        return numrows, rowlen, query_str, res_inserts
    
    def execute_q1(self):
        tname = 'WhatProfile'
        dname = '/home/pranav/catalog_experiments/' + tname
        query_num = 1
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
                        newrow.append(str(datetime.datetime.now()))
                    else:
                        newrow.append(c)
                    
                inserts.append(newrow)
        #construct the query
        if numrows == 0:
            return
        query = 'INSERT INTO ' + tname.lower() + ' VALUES ('
        for i in range(rowlen):
            query += '?,'
        query = query[:-1] + ');'
        print("Executing query 1: " + query)
        cur = self.con.cursor()
        # only profile the query execution part
        pr = cProfile.Profile()
        pr.enable()
        cur.executemany(query, inserts)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q1_nsql_test.txt', 'w+') as f:
            f.write(s.getvalue())
        
        #we need to retrieve the runtime from the statistics file we wrote
        #before we can get the inserts/sec
        #queries_per_sec = float(num_ins) / runtime
    
    def execute_q2(self):
        asset_id = self.get_lastId('Asset')
        profid = self.get_lastId('HowProfile')
        schema = '\"{ denoisingProcedure : run denoise.py with normalize set to true }\" '
        version = 1
        uid = 1
        query_str = 'INSERT INTO howprofile VALUES ('
        
        query_str += str(profid + 1) + ', ' + str(version) + ', ' + '\"' + str(datetime.datetime.now()) + '\"' + ', '
        query_str += str(uid) + ', ' + str(asset_id) + ', ' + str(schema) + ');'
        
        print("Executing Query 2: " + query_str)
        
        cur = self.con.cursor()
        
        pr = cProfile.Profile()
        pr.enable()
        cur.execute(query_str)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q2_nsql_test.txt', 'w+') as f:
            f.write(s.getvalue())
    
    #we need a slightly different helper for q3
    def get_q3inserts(self, x, dname, tname, tsize, asset, jsonval=None):        
        rowlen = 0
        numrows = 0
        res_inserts = []
        cur = self.con.cursor()
        schema = cur.execute("PRAGMA table_info('" + tname + "')").fetchall()
        vertup = [i for i in schema if 'version' in i[1]]
        ver_ind = vertup[0][0]
        timetup = [i for i in schema if 'timestamp' in i[1]]
        time_ind = timetup[0][0]
        assettup = [i for i in schema if 'asset_id' in i[1]]
        asset_ind = assettup[0][0]
        if jsonval != None:
            jsontup = [i for i in schema if 'schema' in i[1]]
            json_ind = jsontup[0][0]
        else:
            json_ind = -1
        
        with open(dname + '.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in csvreader:
                if numrows >= x:
                    break
                rowlen = len(row)
                numrows += 1
                if rowlen == 0:
                    continue
                newrow = []
                for i,c in enumerate(row):
                    if i == 0:
                        newrow.append(str(tsize + numrows))
                    elif i == ver_ind: #update version
                        newrow.append(str(int(c) + 1))
                    elif i == time_ind: #update timestamp
                        newrow.append(str(datetime.datetime.now()))
                    elif i == asset_ind:
                        newrow.append(str(asset))
                    elif jsonval != None and i == json_ind:
                        newrow.append(jsonval)
                    else:
                        newrow.append(c)
                    
                res_inserts.append(newrow)
        
        if numrows == 0:
            return None
        
        query_str = 'INSERT INTO ' + tname.lower() + ' VALUES ('
        for i in range(rowlen):
            query_str += '?,'
        query_str = query_str[:-1] + ');'
        
        return numrows, rowlen, query_str, res_inserts
    
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
        #schemas
        who_schema = '\"{ curator : Joe Schmoe }\"'
        how_schema = '\"{ curationProcess : Replaced all empty strings or negative numbers with NULLs }\"'
        why_schema = '\"{ curationPurpose : We need this dataset cleaned before we can run prediction on it. }\"'
        
        #we'll have the same why_schema for all whyprofiles...
        #and we have to do whyprofile manually because csv file for whyprofile
        #is empty.
        cur = self.con.cursor()
        schema = cur.execute("PRAGMA table_info('" + why_name + "')").fetchall()
        vertup = [i for i in schema if 'version' in i[1]]
        ver_ind = vertup[0][0]
        timetup = [i for i in schema if 'timestamp' in i[1]]
        time_ind = timetup[0][0]
        jsontup = [i for i in schema if 'schema' in i[1]]
        json_ind = jsontup[0][0]
        assettup = [i for i in schema if 'asset_id' in i[1]]
        asset_ind = assettup[0][0]
        why_usertup = [i for i in schema if 'user_id' in i[1]]
        why_user_ind = why_usertup[0][0]
        why_inserts = []
        why_query = 'INSERT INTO ' + why_name.lower() + ' VALUES ('
        for i in range(len(schema)):
            why_query += '?,'
        why_query = why_query[:-1] + ');'
        numrows = 0
        for i in range(x):
            newrow = []
            for j in range(len(schema)):
                if j == 0:
                    numrows += 1
                    newrow.append(str(why_id + numrows))
                elif j == ver_ind:
                    newrow.append(str(i+1))
                elif j == time_ind:
                    newrow.append(str(datetime.datetime.now()))
                elif j == json_ind:
                    newrow.append(why_schema)
                elif j == why_user_ind:
                    newrow.append(str(1))
                elif j == asset_ind:
                    newrow.append(common_asset)
            why_inserts.append(newrow)
            
        #get the inserts for all the other tables
        who_rows, who_len, who_query, who_inserts = self.get_q3inserts(x, '/home/pranav/catalog_experiments/'+ who_name, who_name, who_id, common_asset, who_schema)
        #get the inserts for all the other tables
        when_rows, when_len, when_query, when_inserts = self.get_q3inserts(x, '/home/pranav/catalog_experiments/'+ when_name, when_name, when_id, common_asset)
        how_rows, how_len, how_query, how_inserts = self.get_q3inserts(x, '/home/pranav/catalog_experiments/'+ how_name, how_name, how_id, common_asset, how_schema)
        
        #now, set actions
        action_inserts = []
        for i in range(x):
            newrow = []
            newrow.append(str(action_id + i+1)) #id
            newrow.append(str(1)) #version
            newrow.append(str(datetime.datetime.now()))
            newrow.append(str(1))
            newrow.append(common_asset)
            newrow.append(who_inserts[i][0]) #who_id
            newrow.append(how_inserts[i][0]) #how_id
            newrow.append(why_inserts[i][0]) #why_id
            newrow.append(when_inserts[i][0]) #when_id
            action_inserts.append(newrow)
        
        action_query = 'INSERT INTO ' + action_name.lower() + ' VALUES ('
        for i in range(9):
            action_query += '?,'
        action_query = action_query[:-1] + ');'
        
        #now, we first need to insert our new fictional asset
        asset_query = 'INSERT INTO asset VALUES (' + str(common_asset) + ', "fictionalAsset", ' + '4, 1, ' + '"' + str(datetime.datetime.now()) + '"' + ', 1);'
        print(asset_query)
        cur.execute(asset_query)
        
        #now, we can insert everything else
        pr = cProfile.Profile()
        pr.enable()
        cur.executemany(who_query, who_inserts)
        cur.executemany(why_query, why_inserts)
        cur.executemany(how_query, how_inserts)
        cur.executemany(when_query, when_inserts)
        cur.executemany(action_query, action_inserts)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q3_nsql_test.txt', 'w+') as f:
            f.write(s.getvalue())
        
    
    def execute_q4(self, trial):
        cur = self.con.cursor()
        pr = cProfile.Profile()
        pr.enable()
        cur.execute('SELECT * FROM whatprofile WHERE timestamp <= date(\'2020-12-20\') AND timestamp >= date(\'2020-11-01\')')
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q4_nsql_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q5(self, trial):
        query_str = "SELECT user.name, howprofile.schema, whenprofile.asset_timestamp, whyprofile.schema "
        query_str += "FROM action INNER JOIN whoprofile ON action.who_id = whoprofile.id "
        query_str += "INNER JOIN whyprofile ON action.why_id = whyprofile.id "
        query_str += "INNER JOIN whenprofile ON action.when_id = whenprofile.id "
        query_str += "INNER JOIN howprofile ON action.how_id = howprofile.id "
        query_str += "INNER JOIN user ON whoprofile.write_user_id;"
        
        cur = self.con.cursor()
        pr = cProfile.Profile()
        pr.enable()
        cur.execute(query_str)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q5_nsql_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q6(self, trial):
        query_str = "SELECT whoprofile.schema, howprofile.schema, whenprofile.asset_timestamp, whyprofile.schema FROM action "
        query_str += "INNER JOIN whoprofile ON action.who_id = whoprofile.id "
        query_str += "INNER JOIN whyprofile ON action.why_id = whyprofile.id "
        query_str += "INNER JOIN whenprofile ON action.when_id = whenprofile.id "
        query_str += "INNER JOIN howprofile ON action.how_id = howprofile.id "
        query_str += "ORDER BY action.timestamp DESC LIMIT 10;"
        
        cur = self.con.cursor()
        pr = cProfile.Profile()
        pr.enable()
        cur.execute(query_str)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q6_nsql_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q7(self, trial):
        query_str = "SELECT asset.name, COUNT(*) FROM howprofile INNER JOIN asset "
        query_str += "ON howprofile.asset_id = asset.id GROUP BY howprofile.asset_id;"
        
        cur = self.con.cursor()
        pr = cProfile.Profile()
        pr.enable()
        cur.execute(query_str)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q7_nsql_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_full(self):
        self.execute_q1()
        self.execute_q2()
        self.execute_q3(100000)
        #repeat the other experiments 5 times
        for i in range(6):
            #we get 2407903 records from executing q4
            self.execute_q4(i)
            self.execute_q5(i)
            self.execute_q6(i)
            self.execute_q7(i)
        
        
        
if __name__ == "__main__":
    run_tests = NSQL_Queries('normalized_synthetic.db', "testres.csv")
    #run_tests.execute_q1()
    #run_tests.execute_q2()
    #run_tests.execute_q3()
    #run_tests.execute_q4()
    #run_tests.execute_q5()
    #run_tests.execute_q6()
    #run_tests.execute_q7()
    run_tests.execute_full()
    # tname = 'HowProfile'
    # tsize = 16655
    # numrows, rowlen, query_str, res_inserts = run_tests.get_inserts('/home/pranav/catalog-service/workingdbs/nsql/' + tname, tname, tsize)
    # print("Executing: " + query_str)
    # for r in res_inserts:
    #     print(r)
    run_tests.close_conn()

