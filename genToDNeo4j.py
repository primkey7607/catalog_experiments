from neo4j import GraphDatabase
import os

class GenNNeo4j:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.attrmap = {}
        self.init_attrmap()
    
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
        
        

