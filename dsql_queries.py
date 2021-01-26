import csv
import datetime
import random
import string
import sqlite3
import sys
import os
import time
import cProfile, pstats
import io

class DSQL_Queries:
    
    def __init__(self, dbfile):
        csv.field_size_limit(int(sys.maxsize/10))
        self.dbfile = dbfile
        self.con = sqlite3.connect(dbfile)
        #make sure the sqlite database is enforcing foreign key constraints
        cur = self.con.cursor()
        cur.execute('PRAGMA foreign_keys = ON;')
        cur.close()
    
    def create_out(self, resfile):
        fh = open(resfile, 'w+')
        initwrite = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        initwrite.writerow(['Query Number', 'Query String', 'Runtime', 'Queries per Sec'])
        fh.close()
    
    def get_lastId(self, tname):
        query_str = "select max(id) from " + tname.lower() + ";"
        cur = self.con.cursor()
        maxlst = cur.execute(query_str).fetchone()
        #we're going to want to see what this is
        print(maxlst)
        return maxlst[0]
    
    def close_conn(self):
        self.con.close()
    
    def make_query(self, tname):
        query = 'INSERT INTO ' + tname.lower() + ' VALUES ('
        con = sqlite3.connect('datavault_synthetic.db')
        cur = con.cursor()
        schema = cur.execute("PRAGMA table_info('" + tname + "')").fetchall()
        con.close()
        rowlen = len(schema)
        for i in range(rowlen):
            query += '?,'
        query = query[:-1] + ');'
        return query
    
    def execute_q1(self, x):
        what_name = 'WhatProfile'
        fh = open(what_name + '.csv', 'r')
        reader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        hname = 'H_WhatProfile'
        sname = 'S_WhatProfile_schema'
        lname = 'L_Asset_WhatProfile'
        h_query = self.make_query(hname)
        s_query = self.make_query(sname)
        l_query = self.make_query(lname)
        h_id = self.get_lastId(hname)
        s_id = self.get_lastId(sname)
        l_id = self.get_lastId(lname)
        h_what = []
        l_what = []
        s_what = []
        for i,row in enumerate(reader):
            if i > x:
                break
            ver = row[1]
            date = row[2]
            uid = row[3]
            aid = row[4]
            schema = row[5]
            h_what.append([h_id + i + 1, ver, date, uid])
            s_what.append([s_id + i + 1, h_id + i + 1, schema, ver, date, uid])
            l_what.append([l_id + i + 1,
                           aid, h_id + i + 1, ver, date, uid])
        print("Executing h_query 1: " + h_query)
        print("Executing l_query 1: " + l_query)
        print("Executing s_query 1: " + s_query)
        cur = self.con.cursor()
        # only profile the query execution part
        pr = cProfile.Profile()
        pr.enable()
        cur.executemany(h_query, h_what)
        cur.executemany(l_query, l_what)
        cur.executemany(s_query, s_what)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q1_dsql_test.txt', 'w+') as f:
            f.write(s.getvalue())
        
        cur.close()
    
    def execute_q2(self):
        #adds howprofiles to the last asset
        h_asset_id = self.get_lastId('H_Asset')
        h_profid = self.get_lastId('H_HowProfile') + 1
        l_profid = self.get_lastId('L_Asset_HowProfile') + 1
        s_profid = self.get_lastId('S_HowProfile_schema') + 1
        
        schema = '\"{ denoisingProcedure : run denoise.py with normalize set to true }\" '
        version = 1
        uid = 1
        h_query_str = 'INSERT INTO h_howprofile VALUES ('
        s_query_str = 'INSERT INTO s_howprofile_schema VALUES ('
        l_query_str = 'INSERT INTO l_asset_howprofile VALUES ('
        
        #query_str += str(profid) + ', ' + str(version) + ', ' + '\"' + str(datetime.datetime.now()) + '\"' + ', '
        #query_str += str(uid) + ', ' + str(asset_id) + ', ' + str(schema) + ');'
        
        h_query_str += str(h_profid) + ', ' + str(version) + ', ' + '\"' 
        h_query_str += str(datetime.datetime.now()) + '\"' + ', ' + str(uid) + ');'
        
        s_query_str += str(s_profid) + ', ' + str(h_profid) + ', ' + str(schema) + ', '
        s_query_str += str(version) + ', '
        s_query_str += '\"' + str(datetime.datetime.now()) + '\"' + ', ' + str(uid) + ');'
        
        l_query_str += str(l_profid) + ', ' + str(h_asset_id) + ', '
        l_query_str += str(h_profid) + ', ' + str(version) + ', '
        l_query_str += '\"' + str(datetime.datetime.now()) + '\"' + ', ' + str(uid) + ');'
        
        print("Executing Query 2:")
        print(h_query_str)
        print(s_query_str)
        print(l_query_str)
        
        cur = self.con.cursor()
        
        pr = cProfile.Profile()
        pr.enable()
        cur.execute(h_query_str)
        cur.execute(s_query_str)
        cur.execute(l_query_str)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q2_dsql_test.txt', 'w+') as f:
            f.write(s.getvalue())
        
        cur.close()
    
    #we need a slightly different helper for q3
    #where we find the inserts for hubs, links, or satellites
    def get_q3inserts(self, dname, tname, tsize, asset, jsonval=None):        
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
                if numrows >= 100000:
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
    
    def insertToDB(self, query_str, queries):
        print("About to Execute: " + query_str)
        cur = self.con.cursor()
        cur.executemany(query_str, queries)
        self.con.commit()
        #close the cursor--maybe we're leaving too many of these around,
        #and that's slowing things down
        cur.close()
    
    #x is the number of new actions (and assets) that we are inserting
    def execute_q3(self, x):
        last_asset = self.get_lastId('H_Asset')
        last_alink = self.get_lastId('L_AssetTypeLink')
        h_recs = []
        l_recs = []
        for i in range(x):
            h_pid = last_asset + i + 1
            l_pid = last_alink + i + 1
            name = ''.join(random.choices(string.ascii_letters, k=12))
            atype = 1 #just make them all the same type
            ver = 1
            date = str(self.random_date())
            uid = 1
            h_recs.append([h_pid, name, ver, date, uid])
            l_recs.append([l_pid, h_pid, atype, ver, date, uid])
        
        #Insert the assets right away--but batch everything else,
        #because we need to time them
        #res_inserts.append((self.make_query('h_asset'), h_recs))
        #res_inserts.append((self.make_query('l_assettypelink'), l_recs))
        self.insertToDB(self.make_query('h_asset'), h_recs)
        self.insertToDB(self.make_query('l_assettypelink'), l_recs)
        
        who_schema = '\"{ curator : Joe Schmoe }\"'
        how_schema = '\"{ curationProcess : Replaced all empty strings or negative numbers with NULLs }\"'
        start = datetime.datetime.strptime('6/1/2020 12:00 AM', '%m/%d/%Y %I:%M %p')
        end = datetime.datetime.now()
        when_dates = self.k_in_time(start, end, 3)
        when_attrs = []
        #convert this to strings
        for d in when_dates:
            when_attrs.append(str(d))
        why_schema = '\"{ curationPurpose : We need this dataset cleaned before we can run prediction on it. }\"'
        
        last_hwho = self.get_lastId('H_WhoProfile')
        last_swho = self.get_lastId('S_WhoProfile_schema')
        last_lawho = self.get_lastId('L_Asset_WhoProfile')
        last_luwho = self.get_lastId('L_WhoProfileUser')
        last_hhow = self.get_lastId('H_HowProfile')
        last_show = self.get_lastId('S_HowProfile_schema')
        last_lhow = self.get_lastId('L_Asset_HowProfile')
        last_hwhy = self.get_lastId('H_WhyProfile')
        last_swhy = self.get_lastId('S_WhyProfile_schema')
        last_lwhy = self.get_lastId('L_Asset_WhyProfile')
        last_hwhen = self.get_lastId('H_WhenProfile')
        last_swhen = self.get_lastId('S_WhenProfile_Attributes')
        last_lwhen = self.get_lastId('L_Asset_WhenProfile')
        last_haction = self.get_lastId('H_Action')
        last_laction = self.get_lastId('L_AssetsInActions')
        
        h_who = []
        s_who = []
        l_who = []
        l_user = []
        h_how = []
        s_how = []
        l_how = []
        h_why = []
        s_why = []
        l_why = []
        h_when = []
        s_when = []
        l_when = []
        h_action = []
        l_action = []
        
        for i in range(x):
            h_whopid = last_hwho + i + 1
            s_whopid = last_swho + i + 1
            la_whopid = last_lawho + i + 1
            lu_whopid = last_luwho + i + 1
            h_howpid = last_hhow + i + 1
            s_howpid = last_show + i + 1
            l_howpid = last_lhow + i + 1
            h_whypid = last_hwhy + i + 1
            s_whypid = last_swhy + i + 1
            l_whypid = last_lwhy + i + 1
            h_whenpid = last_hwhen + i + 1
            s_whenpid = last_swhen + i + 1
            l_whenpid = last_lwhen + i + 1
            h_actionid = last_haction + i + 1
            l_actionid = last_laction + i + 1
            ver = 1
            date = str(self.random_date())
            write_user = 1
            aid = random.choice(range(last_asset + 1, last_asset + 1 + x))
            uid = 1
            h_who.append([h_whopid, ver, date, write_user])
            s_who.append([s_whopid, h_whopid, who_schema, uid, ver, date, write_user])
            l_who.append([la_whopid, aid, h_whopid, ver, date, write_user])
            l_user.append([lu_whopid, h_whopid, uid, ver, date, write_user])
            h_how.append([h_howpid, ver, date, uid])
            s_how.append([s_howpid, h_howpid, how_schema, ver, date, uid])
            l_how.append([l_howpid, aid, h_howpid, ver, date, uid])
            h_why.append([h_whypid, ver, date, uid])
            s_why.append([s_whypid, h_whypid, why_schema, ver, date, uid])
            l_why.append([l_whypid, aid, h_whypid, ver, date, uid])
            h_when.append([h_whenpid, ver, date, uid])
            s_when.append([s_whenpid, h_whenpid, when_attrs[1], when_attrs[2],
                           when_attrs[0], ver, date, uid])
            l_when.append([l_whenpid, aid, h_whenpid, ver, date, uid])
            h_action.append([h_actionid, ver, date, uid])
            l_action.append([l_actionid, h_actionid, aid, h_whopid,
                             h_whypid, h_whenpid, h_howpid, ver, date, uid])
            
            
            
        h_who_query = self.make_query('h_whoprofile')
        s_who_query = self.make_query('s_whoprofile_schema')
        l_awho_query = self.make_query('l_asset_whoprofile')
        l_uwho_query = self.make_query('l_whoprofileuser')
        h_how_query = self.make_query('h_howprofile')
        s_how_query = self.make_query('s_howprofile_schema')
        l_how_query = self.make_query('l_asset_howprofile')
        h_why_query = self.make_query('h_whyprofile')
        s_why_query = self.make_query('s_whyprofile_schema')
        l_why_query = self.make_query('l_asset_whyprofile')
        h_when_query = self.make_query('h_whenprofile')
        s_when_query = self.make_query('s_whenprofile_attributes')
        l_when_query = self.make_query('l_asset_whenprofile')
        h_action_query = self.make_query('h_action')
        l_action_query = self.make_query('l_assetsinactions')
        
        #now, we can insert everything else
        os.system('echo 1 | sudo tee /proc/sys/vm/drop_caches')
        time.sleep(5)
        print("Length of who_query: " + str(len(h_who)))
        print("Length of who_query: " + str(len(s_who)))
        print("Length of who_query: " + str(len(l_who)))
        print("Length of user_who: " + str(len(l_user)))
        print("Length of why_query: " + str(len(h_why)))
        print("Length of why_query: " + str(len(s_why)))
        print("Length of why_query: " + str(len(l_why)))
        print("Length of how_query: " + str(len(h_how)))
        print("Length of how_query: " + str(len(s_how)))
        print("Length of how_query: " + str(len(l_how)))
        print("Length of when_query: " + str(len(h_when)))
        print("Length of when_query: " + str(len(s_when)))
        print("Length of when_query: " + str(len(l_when)))
        print("Length of action_query: " + str(len(h_action)))
        print("Length of action_query: " + str(len(l_action)))
        cur = self.con.cursor()
        pr = cProfile.Profile()
        pr.enable()
        cur.executemany(h_who_query, h_who)
        cur.executemany(s_who_query, s_who)
        cur.executemany(l_awho_query, l_who)
        cur.executemany(l_uwho_query, l_user)
        cur.executemany(h_why_query, h_why)
        cur.executemany(s_why_query, s_why)
        cur.executemany(l_why_query, l_why)
        cur.executemany(h_how_query, h_how)
        cur.executemany(s_how_query, s_how)
        cur.executemany(l_how_query, l_how)
        cur.executemany(h_when_query, h_when)
        cur.executemany(s_when_query, s_when)
        cur.executemany(l_when_query, l_when)
        cur.executemany(h_action_query, h_action)
        cur.executemany(l_action_query, l_action)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q3_dsql_test.txt', 'w+') as f:
            f.write(s.getvalue())
        
        cur.close()
    
    def execute_q4(self, trial):
        #reinitialize the db connection
        self.con.close()
        self.con = sqlite3.connect(self.dbfile)
        cur = self.con.cursor()
        q4str = 'SELECT * FROM h_whatprofile '
        q4str += 'INNER JOIN s_whatprofile_schema ON h_whatprofile.id = s_whatprofile_schema.what_profile_id '
        q4str += 'WHERE h_whatprofile.timestamp <= date(\'2020-12-20\') AND h_whatprofile.timestamp >= date(\'2020-11-01\');'
        pr = cProfile.Profile()
        pr.enable()
        cur.execute(q4str)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q4_dsql_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q5(self, trial):
        query_str = "SELECT h_user.name, s_howprofile_schema.schema, s_whenprofile_attributes.asset_timestamp, s_whyprofile_schema.schema "
        query_str += "FROM l_assetsinactions INNER JOIN h_whoprofile ON l_assetsinactions.who_profile_id = h_whoprofile.id "
        query_str += "INNER JOIN s_whoprofile_schema ON s_whoprofile_schema.who_profile_id = h_whoprofile.id "
        query_str += "INNER JOIN h_whyprofile ON l_assetsinactions.why_profile_id = h_whyprofile.id "
        query_str += "INNER JOIN s_whyprofile_schema ON s_whyprofile_schema.why_profile_id = h_whyprofile.id "
        query_str += "INNER JOIN h_whenprofile ON l_assetsinactions.when_profile_id = h_whenprofile.id "
        query_str += "INNER JOIN s_whenprofile_attributes ON s_whenprofile_attributes.h_when_id = h_whenprofile.id "
        query_str += "INNER JOIN h_howprofile ON l_assetsinactions.how_profile_id = h_howprofile.id "
        query_str += "INNER JOIN s_howprofile_schema ON h_howprofile.id = s_howprofile_schema.how_profile_id "
        query_str += "INNER JOIN l_whoprofileuser ON l_whoprofileuser.who_profile_id = h_whoprofile.id "
        query_str += "INNER JOIN h_user ON l_whoprofileuser.who_user_id = h_user.id;"
        
        #reinitialize the db connection
        self.con.close()
        self.con = sqlite3.connect(self.dbfile)
        cur = self.con.cursor()
        pr = cProfile.Profile()
        pr.enable()
        cur.execute(query_str)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q5_dsql_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q6(self, trial):
        query_str = "SELECT s_whoprofile_schema.schema, s_howprofile_schema.schema, s_whenprofile_attributes.asset_timestamp, s_whyprofile_schema.schema "
        query_str += "FROM l_assetsinactions INNER JOIN h_whoprofile ON l_assetsinactions.who_profile_id = h_whoprofile.id "
        query_str += "INNER JOIN s_whoprofile_schema ON s_whoprofile_schema.who_profile_id = h_whoprofile.id "
        query_str += "INNER JOIN h_whyprofile ON l_assetsinactions.why_profile_id = h_whyprofile.id "
        query_str += "INNER JOIN s_whyprofile_schema ON s_whyprofile_schema.why_profile_id = h_whyprofile.id "
        query_str += "INNER JOIN h_whenprofile ON l_assetsinactions.when_profile_id = h_whenprofile.id "
        query_str += "INNER JOIN s_whenprofile_attributes ON s_whenprofile_attributes.h_when_id = h_whenprofile.id "
        query_str += "INNER JOIN h_howprofile ON l_assetsinactions.how_profile_id = h_howprofile.id "
        query_str += "INNER JOIN s_howprofile_schema ON h_howprofile.id = s_howprofile_schema.how_profile_id "
        query_str += "ORDER BY l_assetsinactions.timestamp DESC LIMIT 10;"
        
        #reinitialize the db connection
        self.con.close()
        self.con = sqlite3.connect(self.dbfile)
        cur = self.con.cursor()
        pr = cProfile.Profile()
        pr.enable()
        cur.execute(query_str)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q6_dsql_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_q7(self, trial):
        query_str = "SELECT h_asset.name, COUNT(*) FROM h_howprofile INNER JOIN l_asset_howprofile "
        query_str += "ON h_howprofile.id = l_asset_howprofile.how_profile_id "
        query_str += "INNER JOIN h_asset ON l_asset_howprofile.asset_id = h_asset.id GROUP BY l_asset_howprofile.asset_id;"
        
        #reinitialize the db connection
        self.con.close()
        self.con = sqlite3.connect(self.dbfile)
        cur = self.con.cursor()
        pr = cProfile.Profile()
        pr.enable()
        cur.execute(query_str)
        pr.disable()
        self.con.commit()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
        ps.print_stats()
        
        with open('q7_dsql_test_' + str(trial) + '.txt', 'w+') as f:
            f.write(s.getvalue())
    
    def execute_full(self):
        #self.execute_q1(100000)
        #self.execute_q2()
        #self.execute_q3(100000)
        #repeat the other experiments 5 times
        for i in range(6):
            os.system('echo 1 | sudo tee /proc/sys/vm/drop_caches')
            time.sleep(5)
            self.execute_q4(i)
            self.execute_q5(i)
            self.execute_q6(i)
            self.execute_q7(i)
    
    
        

if __name__ == "__main__":
    run_tests = DSQL_Queries('datavault_synthetic.db')
    #run_tests.execute_full()
    run_tests.execute_q3(100000)
    run_tests.close_conn()
