import csv
import datetime
import random
import string
import sys

#The point of this file is to convert the existing normalized dataset
#into datavault files with neo4j-valid datetimes

class GenDNeoFiles:
    def __init__(self):
        self.version = 1
        self.con = None
        self.asset_names = []
        self.ins_map = {}
        self.keymap = {}
        self.init_keymap()
        self.csv_map = {}
        self.fh_map = {}
        self.create_readers()
    
    def convert_date(self, dt):
        dsplit = dt.split(' ')
        valid = 'T'.join(dsplit)
        return valid
    
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
    
    def insertToFile(self, tname, csvwriter, recs):
        csvwriter.writerows(recs)
    
    #there's pretty much no way to factor this nicely...
    def load_usertype(self):
        h_usertype_recs = []
        s_usertype_recs = []
        
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_UserType.csv', 'w+')
        s_fh = open('S_UserTypeAttributes', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('UserType.csv', 'r') as fh:
            for i,row in enumerate(fh):
                rowlen = len(row)
                if rowlen == 0:
                    continue
                utype_id = row[0]
                utype_name = row[1]
                utype_desc = row[2]
                timestamp = self.random_date()
                timestamp = self.convert_date(str(timestamp))
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
                    self.insertToFile('H_UserType', h_writer, h_usertype_recs)
                    h_usertype_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_UserTypeAttributes', s_writer, s_usertype_recs)
                    s_usertype_recs = []
                    s_chunksize = 0
            
                #insert any leftovers:
            if len(h_usertype_recs) > 0:
                self.insertToFile('H_UserType', h_writer, h_usertype_recs)
            if len(s_usertype_recs) > 0:
                self.insertToFile('S_UserTypeAttributes', s_writer, s_usertype_recs)
        
        h_fh.close()
        s_fh.close()
    

if __name__ == "__main__":
    dgen = GenDNeoFiles()
    dgen.load_usertype()

