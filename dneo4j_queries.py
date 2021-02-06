from neo4j import GraphDatabase
import os
import csv
import datetime
from neo4j.time import DateTime
import sys
import time
import cProfile, pstats
import io

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
            #we already created uniqueness constraints for these
            if tname == 'WhatProfile' or tname == 'User' or tname == 'Asset':
                continue
            query_str = "CREATE CONSTRAINT ON (p:" + tname + ") "
            query_str += "ASSERT p.id IS UNIQUE"
            with self.driver.session() as session:
                session.run(query_str)
    
    def make_q1_query(self, tname):
        query_str1 = "UNWIND $props AS map CREATE (n:" + tname + ") SET n = map"
        query_str1 += " RETURN n"
        query_str2 = "MATCH (m:User) WHERE m.id = n.user_id CREATE (n)-[:Rel_WhatProfile_User]->(m)"
        
        query_str = 'call apoc.periodic.iterate("' + query_str1 + '", '
        query_str += '"' + query_str2 + '", {batchSize:1000}) YIELD batches, total, errorMessages return batches, total, errorMessages'
        
        return query_str

if __name__ == "__main__":
    neo_queries = DNeo4j_Queries("bolt://localhost:7687", "neo4j", "normal")
    neo_queries.create_indexes()