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
    
    def convert_date(self, dt):
        dsplit = dt.split(' ')
        if len(dsplit) != 2:
            print("Assuming date format is correct: " + str(dt))
            return dt
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
        final_date = start + datetime.timedelta(seconds=random_second)
        result = self.convert_date(str(final_date))
        return result
    
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
        s_fh = open('S_UserTypeAttributes.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/UserType.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
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
        
    def load_assettype(self):
        h_assettype_recs = []
        s_assettype_recs = []
        
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_AssetType.csv', 'w+')
        s_fh = open('S_AssetTypeAttributes.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/AssetType.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                atype_id = row[0]
                atype_name = row[1]
                atype_desc = row[2]
                timestamp = self.random_date()
                timestamp = self.convert_date(str(timestamp))
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
                    self.insertToFile('H_AssetType', h_writer, h_assettype_recs)
                    h_assettype_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_AssetTypeAttributes', s_writer, s_assettype_recs)
                    s_assettype_recs = []
                    s_chunksize = 0
            
            #insert any leftovers:
            if len(h_assettype_recs) > 0:
                self.insertToFile('H_AssetType', h_writer, h_assettype_recs)
            if len(s_assettype_recs) > 0:
                self.insertToFile('S_AssetTypeAttributes', s_writer, s_assettype_recs)
    
    def load_reltype(self):
        h_relationshiptype_recs = []
        s_relationshiptype_recs = []
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_RelationshipType.csv', 'w+')
        s_fh = open('S_RelationshipTypeAttributes.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/RelationshipType.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i, row in enumerate(csvreader):
                pid = row[0]
                name = row[1]
                desc = row[2]
                timestamp = self.random_date()
                timestamp = self.convert_date(str(timestamp))
                user_id = 1
                h_relationshiptype_recs.append([pid, self.version, timestamp, user_id])
                s_relationshiptype_recs.append([pid, name, desc, self.version, timestamp, user_id])
                h_chunksize += 1
                s_chunksize += 1
                if h_chunksize >= 1000:
                    h_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting h_Chunk Number: " + str(h_chunknum))
                    self.insertToFile('H_RelationshipType', h_writer, h_relationshiptype_recs)
                    h_relationshiptype_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_RelationshipTypeAttributes', s_writer, s_relationshiptype_recs)
                    s_relationshiptype_recs = []
                    s_chunksize = 0
            
            #insert any leftovers:
            if len(h_relationshiptype_recs) > 0:
                self.insertToFile('H_RelationshipType', h_writer, h_relationshiptype_recs)
            if len(s_relationshiptype_recs) > 0:
                self.insertToFile('S_RelationshipTypeAttributes', s_writer, s_relationshiptype_recs)
                
    def load_sourcetype(self):
        h_sourcetype_recs = []
        s_sourcetype_recs = []
        h_chunksize = 0
        s_chunksize = 0
        h_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_SourceType.csv', 'w+')
        s_fh = open('S_SourceTypeAttributes.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/SourceType.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                connector = row[1]
                serde = row[2]
                datamodel = row[3]
                timestamp = self.random_date()
                timestamp = self.convert_date(str(timestamp))
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
                    self.insertToFile('H_SourceType', h_writer, h_sourcetype_recs)
                    h_sourcetype_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_SourceTypeAttributes', s_writer, s_sourcetype_recs)
                    s_sourcetype_recs = []
                    s_chunksize = 0
            
            #insert any leftovers:
            if len(h_sourcetype_recs) > 0:
                self.insertToFile('H_SourceType', h_writer, h_sourcetype_recs)
            if len(s_sourcetype_recs) > 0:
                self.insertToFile('S_SourceTypeAttributes', s_writer, s_sourcetype_recs)
    
    #create all users, as before
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
        h_fh = open('H_User.csv', 'w+')
        s_fh = open('S_User_schema.csv', 'w+')
        l_fh = open('L_UserTypeLink.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/User.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                name = row[1]
                utype = row[2]
                schema = row[3]
                ver = row[4]
                date = row[5]
                date = self.convert_date(str(date))
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
                    self.insertToFile('H_User', h_writer, h_user_recs)
                    h_user_recs = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_User_schema', s_writer, s_user_recs)
                    s_user_recs = []
                    s_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToFile('L_UserTypeLink', l_writer, l_user_recs)
                    l_user_recs = []
                    l_chunksize = 0
        
            #insert any leftovers:
            if len(h_user_recs) > 0:
                self.insertToFile('H_User', h_writer, h_user_recs)
            if len(s_user_recs) > 0:
                self.insertToFile('S_User_schema', s_writer, s_user_recs)
            if len(l_user_recs) > 0:
                self.insertToFile('L_UserTypeLink', l_writer, l_user_recs)
    
    def load_assets(self):
        h_recs = []
        l_recs = []
        
        h_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        h_fh = open('H_Asset.csv', 'w+')
        l_fh = open('L_AssetTypeLink.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/Asset.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                name = row[1]
                atype = row[2]
                ver = row[3]
                date = row[4]
                date = self.convert_date(str(date))
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
                    self.insertToFile('H_Asset', h_writer, h_recs)
                    h_recs = []
                    h_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToFile('L_AssetTypeLink', l_writer, l_recs)
                    l_recs = []
                    l_chunksize = 0
            
            #insert any leftovers:
            if len(h_recs) > 0:
                self.insertToFile('H_Asset', h_writer, h_recs)
            if len(l_recs) > 0:
                self.insertToFile('L_AssetTypeLink', l_writer, l_recs)
    
    def load_sources(self):
        h_recs = []
        l_recs = []
        s_recs = []
        
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_Source.csv', 'w+')
        s_fh = open('S_Source_schema.csv', 'w+')
        l_fh = open('L_Source2Type.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        with open('10gbfiles/Source.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                ver = row[1]
                date = row[2]
                date = self.convert_date(str(date))
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
                    self.insertToFile('H_Source', h_writer, h_recs)
                    h_recs = []
                    h_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToFile('L_Source2Type', l_writer, l_recs)
                    l_recs = []
                    l_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_Source_schema', s_writer, s_recs)
                    s_recs = []
                    s_chunksize = 0
            
            #insert any leftovers:
            if len(h_recs) > 0:
                self.insertToFile('H_Source', h_writer, h_recs)
            if len(l_recs) > 0:
                self.insertToFile('L_Source2Type', l_writer, l_recs)
            if len(s_recs) > 0:
                self.insertToFile('S_Source_schema', s_writer, s_recs)
    
    def load_relationships(self):
        h_recs = []
        l_recs = []
        l_ar = []
        s_recs = []
        uids = []
        
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        la_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        la_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_Relationship.csv', 'w+')
        s_fh = open('S_Relationship_schema.csv', 'w+')
        l_fh = open('L_Relationship_Type.csv', 'w+')
        la_fh = open('L_Asset_Relationships.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        la_writer = csv.writer(la_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/Relationship.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                ver = row[1]
                date = row[2]
                date = self.convert_date(str(date))
                uid = row[3]
                #print("Appending UID number: " + str(i))
                uids.append(uid)
                r_type = row[4]
                schema = row[5]
                h_recs.append([pid, ver, date, uid])
                s_recs.append([pid, pid, schema, ver, date, uid])
                self.keymap["L_Relationship_Type"] += 1
                l_recs.append([self.keymap['L_Relationship_Type'],
                                pid, r_type, ver, date, uid])
                h_chunksize += 1
                l_chunksize += 1
                s_chunksize += 1
                if h_chunksize >= 1000:
                    h_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting h_Chunk Number: " + str(h_chunknum))
                    self.insertToFile('H_Relationship', h_writer, h_recs)
                    h_recs = []
                    h_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToFile('L_Relationship_Type', l_writer, l_recs)
                    l_recs = []
                    l_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_Relationship_schema', s_writer, s_recs)
                    s_recs = []
                    s_chunksize = 0
            
            #insert any leftovers:
            if len(h_recs) > 0:
                self.insertToFile('H_Relationship', h_writer, h_recs)
            if len(l_recs) > 0:
                self.insertToFile('L_Relationship_Type', l_writer, l_recs)
            if len(s_recs) > 0:
                self.insertToFile('S_Relationship_schema', s_writer, s_recs)
        
        with open('10gbfiles/Asset_Relationships.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
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
                    print("Inserting la_Chunk Number: " + str(la_chunknum))
                    self.insertToFile('L_Asset_Relationships', la_writer, l_ar)
                    l_ar = []
                    la_chunksize = 0
            
            #insert any leftovers
            if len(l_ar) > 0:
                self.insertToFile('L_Asset_Relationships', la_writer, l_ar)
        
        h_fh.close()
        s_fh.close()
        l_fh.close()
        la_fh.close()
    
    def load_whoprofile(self):
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
        h_fh = open('H_WhoProfile.csv', 'w+')
        s_fh = open('S_WhoProfile_schema.csv', 'w+')
        l_fh = open('L_Asset_WhoProfile.csv', 'w+')
        lu_fh = open('L_WhoProfileUser.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        lu_writer = csv.writer(lu_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/WhoProfile.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                ver = row[1]
                date = row[2]
                date = self.convert_date(str(date))
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
                    self.insertToFile('H_WhoProfile', h_writer, h_who)
                    h_who = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_WhoProfile_schema', s_writer, s_who)
                    s_who = []
                    s_chunksize = 0
                if la_chunksize >= 1000:
                    la_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(la_chunknum))
                    self.insertToFile('L_Asset_WhoProfile', l_writer, l_who)
                    l_who = []
                    la_chunksize = 0
                if lu_chunksize >= 1000:
                    lu_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(lu_chunknum))
                    self.insertToFile('L_WhoProfileUser', lu_writer, l_user)
                    l_user = []
                    lu_chunksize = 0
            
            #insert any leftovers:
            if len(h_who) > 0:
                self.insertToFile('H_WhoProfile', h_writer, h_who)
            if len(s_who) > 0:
                self.insertToFile('S_WhoProfile_schema', s_writer, s_who)
            if len(l_who) > 0:
                self.insertToFile('L_Asset_WhoProfile', l_writer, l_who)
            if len(l_user) > 0:
                self.insertToFile('L_WhoProfileUser', lu_writer, l_user)
        
        h_fh.close()
        s_fh.close()
        l_fh.close()
        lu_fh.close()
    
    def load_whatprofile(self):
        h_what = []
        s_what = []
        l_what = []
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_WhatProfile.csv', 'w+')
        s_fh = open('S_WhatProfile_schema.csv', 'w+')
        l_fh = open('L_Asset_WhatProfile.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        with open('10gbfiles/WhatProfile.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                ver = row[1]
                date = row[2]
                date = self.convert_date(date)
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
                    self.insertToFile('H_WhatProfile', h_writer, h_what)
                    h_what = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_WhatProfile_schema', s_writer, s_what)
                    s_what = []
                    s_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToFile('L_Asset_WhatProfile', l_writer, l_what)
                    l_what = []
                    l_chunksize = 0
            
            #insert any leftovers:
            if len(h_what) > 0:
                self.insertToFile('H_WhatProfile', h_writer, h_what)
            if len(s_what) > 0:
                self.insertToFile('S_WhatProfile_schema', s_writer, s_what)
            if len(l_what) > 0:
                self.insertToFile('L_Asset_WhatProfile', l_writer, l_what)
        
        h_fh.close()
        s_fh.close()
        l_fh.close()
    
    def load_howprofile(self):
        h_how = []
        s_how = []
        l_how = []
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_HowProfile.csv', 'w+')
        s_fh = open('S_HowProfile_schema.csv', 'w+')
        l_fh = open('L_Asset_HowProfile.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/HowProfile.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                ver = row[1]
                date = row[2]
                date = self.convert_date(str(date))
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
                    self.insertToFile('H_HowProfile', h_writer, h_how)
                    h_how = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_HowProfile_schema', s_writer, s_how)
                    s_how = []
                    s_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToFile('L_Asset_HowProfile', l_writer, l_how)
                    l_how = []
                    l_chunksize = 0
            
            #insert any leftovers:
            if len(h_how) > 0:
                self.insertToFile('H_HowProfile', h_writer, h_how)
            if len(s_how) > 0:
                self.insertToFile('S_HowProfile_schema', s_writer, s_how)
            if len(l_how) > 0:
                self.insertToFile('L_Asset_HowProfile', l_writer, l_how)
        
        h_fh.close()
        s_fh.close()
        l_fh.close()
    
    def load_whyprofile(self):
        h_why = []
        s_why = []
        l_why = []
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_WhyProfile.csv', 'w+')
        s_fh = open('S_WhyProfile_schema.csv', 'w+')
        l_fh = open('L_Asset_WhyProfile.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/WhyProfile.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                ver = row[1]
                date = row[2]
                date = self.convert_date(str(date))
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
                    self.insertToFile('H_WhyProfile', h_writer, h_why)
                    h_why = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_WhyProfile_schema', s_writer, s_why)
                    s_why = []
                    s_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToFile('L_Asset_WhyProfile', l_writer, l_why)
                    l_why = []
                    l_chunksize = 0
            
            #insert any leftovers:
            if len(h_why) > 0:
                self.insertToFile('H_WhyProfile', h_writer, h_why)
            if len(s_why) > 0:
                self.insertToFile('S_WhyProfile_schema', s_writer, s_why)
            if len(l_why) > 0:
                self.insertToFile('L_Asset_WhyProfile', l_writer, l_why)
        
        h_fh.close()
        s_fh.close()
        l_fh.close()
    
    def load_whenprofile(self):
        h_when = []
        s_when = []
        l_when = []
        h_chunksize = 0
        s_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        s_chunknum = 0
        h_fh = open('H_WhenProfile.csv', 'w+')
        s_fh = open('S_WhenProfile_Attributes.csv', 'w+')
        l_fh = open('L_Asset_WhenProfile.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/WhenProfile.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                ver = row[1]
                date = row[2]
                date = self.convert_date(str(date))
                uid = row[3]
                aid = row[4]
                a_time = row[5]
                a_time = self.convert_date(str(a_time))
                expiry = row[6]
                expiry = self.convert_date(str(expiry))
                start = row[7]
                start = self.convert_date(str(start))
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
                    self.insertToFile('H_WhenProfile', h_writer, h_when)
                    h_when = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_WhenProfile_Attributes', s_writer, s_when)
                    s_when = []
                    s_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToFile('L_Asset_WhenProfile', l_writer, l_when)
                    l_when = []
                    l_chunksize = 0
            
            #insert any leftovers:
            if len(h_when) > 0:
                self.insertToFile('H_WhenProfile', h_writer, h_when)
            if len(s_when) > 0:
                self.insertToFile('S_WhenProfile_Attributes', s_writer, s_when)
            if len(l_when) > 0:
                self.insertToFile('L_Asset_WhenProfile', l_writer, l_when)
        h_fh.close()
        l_fh.close()
        s_fh.close()
    
    def load_whereprofile(self):
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
        h_fh = open('H_WhereProfile.csv', 'w+')
        s_fh = open('S_Configuration.csv', 'w+')
        l_fh = open('L_Asset_WhereProfile.csv', 'w+')
        ls_fh = open('L_WhereProfile_Source.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        s_writer = csv.writer(s_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        ls_writer = csv.writer(ls_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/WhereProfile.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                ver = row[1]
                date = row[2]
                date = self.convert_date(str(date))
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
                    self.insertToFile('H_WhereProfile', h_writer, h_where)
                    h_where = []
                    h_chunksize = 0
                if s_chunksize >= 1000:
                    s_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting s_Chunk Number: " + str(s_chunknum))
                    self.insertToFile('S_Configuration', s_writer, s_where)
                    s_where = []
                    s_chunksize = 0
                if la_chunksize >= 1000:
                    la_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(la_chunknum))
                    self.insertToFile('L_Asset_WhereProfile', l_writer, l_where)
                    l_where = []
                    la_chunksize = 0
                if ls_chunksize >= 1000:
                    ls_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(ls_chunknum))
                    self.insertToFile('L_WhereProfile_Source', ls_writer, l_2s)
                    l_2s = []
                    ls_chunksize = 0
                
            
            #insert any leftovers:
            if len(h_where) > 0:
                self.insertToFile('H_WhereProfile', h_writer, h_where)
            if len(s_where) > 0:
                self.insertToFile('S_Configuration', s_writer, s_where)
            if len(l_where) > 0:
                self.insertToFile('L_Asset_WhereProfile', l_writer, l_where)
            if len(l_2s) > 0:
                self.insertToFile('L_WhereProfile_Source', ls_writer, l_2s)
        
        h_fh.close()
        s_fh.close()
        l_fh.close()
        ls_fh.close()
    
    def load_actions(self):
        h_actions = []
        l_actions = []
        h_chunksize = 0
        l_chunksize = 0
        h_chunknum = 0
        l_chunknum = 0
        h_fh = open('H_Action.csv', 'w+')
        l_fh = open('L_AssetsInActions.csv', 'w+')
        h_writer = csv.writer(h_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        l_writer = csv.writer(l_fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open('10gbfiles/Action.csv', 'r') as fh:
            csvreader = csv.reader(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for i,row in enumerate(csvreader):
                pid = row[0]
                ver = row[1]
                date = row[2]
                date = self.convert_date(str(date))
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
                    self.insertToFile('H_Action', h_writer, h_actions)
                    h_actions = []
                    h_chunksize = 0
                if l_chunksize >= 1000:
                    l_chunknum += 1
                    print("Inserting through row: " + str(i))
                    print("Inserting l_Chunk Number: " + str(l_chunknum))
                    self.insertToFile('L_AssetsInActions', l_writer, l_actions)
                    l_actions = []
                    l_chunksize = 0
            
            if len(h_actions) > 0:
                self.insertToFile('H_Action', h_writer, h_actions)
            if len(l_actions) > 0:
                self.insertToFile('L_AssetsInActions', l_writer, l_actions)
        
    def load_all_files(self):
        self.load_usertype()
        self.load_assettype()
        self.load_sourcetype()
        self.load_users()
        self.load_assets()
        self.load_sources()
        self.load_relationships()
        self.load_whoprofile()
        self.load_whatprofile()
        self.load_howprofile()
        self.load_whyprofile()
        self.load_whenprofile()
        self.load_whereprofile()
        self.load_actions()
        

if __name__ == "__main__":
    dgen = GenDNeoFiles()
    #dgen.load_usertype()
    #dgen.load_assettype()
    #dgen.load_sourcetype()
    #dgen.load_users()
    #dgen.load_assets()
    #dgen.load_sources()
    #dgen.load_relationships()
    #dgen.load_whoprofile()
    #dgen.load_whatprofile()
    #dgen.load_howprofile()
    #dgen.load_whyprofile()
    #dgen.load_whenprofile()
    #dgen.load_whereprofile()
    #dgen.load_actions()

