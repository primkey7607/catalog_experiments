import csv

class DVImportCreator:
    
    def __init__(self):
        self.attrmap = {}
        self.init_attrmap()
        self.fkmap = {}
        self.init_fkmap()
        self.write_user_tables = ['L_UserTypeLink', 'S_User_schema',
                            'S_WhoProfile_schema']
        self.linkmap = {}
        self.init_linkmap()
        self.link2rel = {}
        for key in self.attrmap:
            if key[:2] == 'L_':
                self.link2rel[key] = []
        self.table2rel = {}
        for key in self.attrmap:
            if key[:2] != 'L_':
                self.table2rel[key] = []
    
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
        
    def init_linkmap(self):
        self.linkmap['L_UserTypeLink'] = [('user_id', 'H_User'), ('user_type_id', 'H_UserType')]
        self.linkmap['L_AssetTypeLink'] = [('asset_id', 'H_Asset'), ('asset_type_id', 'H_AssetType')]
        self.linkmap['L_Asset_WhoProfile'] = [('who_profile_id', 'H_WhoProfile'), ('asset_id', 'H_Asset')]
        self.linkmap['L_WhoProfileUser'] = [('who_profile_id', 'H_WhoProfile'), ('who_user_id', 'H_User')]
        self.linkmap['L_Asset_HowProfile'] = [('how_profile_id', 'H_HowProfile'),
                                            ('asset_id', 'H_Asset')]
        self.linkmap['L_Asset_WhyProfile'] = [('why_profile_id', 'H_WhyProfile'), ('asset_id', 'H_Asset')]
        self.linkmap['L_Asset_WhatProfile'] = [('what_profile_id', 'H_WhatProfile'), ('asset_id', 'H_Asset')]
        self.linkmap['L_Asset_WhenProfile'] = [('when_profile_id', 'H_WhenProfile'), ('asset_id', 'H_Asset')]
        self.linkmap['L_Source2Type'] = [('source_id', 'H_Source'), ('source_type_id', 'H_SourceType')]
        self.linkmap['L_Asset_WhereProfile'] = [('where_profile_id', 'H_WhereProfile'),
                                                ('asset_id', 'H_Asset')]
        self.linkmap['L_AssetsInActions'] = [[('action_id', 'H_Action'), ('asset_id', 'H_Asset')],
                                             [('action_id', 'H_Action'), ('who_profile_id', 'H_WhoProfile')],
                                             [('action_id', 'H_Action'), ('why_profile_id', 'H_WhyProfile')],
                                             [('action_id', 'H_Action'), ('when_profile_id', 'H_WhenProfile')],
                                             [('action_id', 'H_Action'), ('how_profile_id', 'H_HowProfile')]]
        self.linkmap['L_Relationship_Type'] = [('relationship_id', 'H_Relationship'), ('relationship_type_id', 'H_RelationshipType')]
        self.linkmap['L_Asset_Relationships'] = [('relationship_id', 'H_Relationship'),
                                                 ('asset_id', 'H_Asset')]
        self.linkmap['L_WhereProfile_Source'] = [('where_profile_id', 'H_WhereProfile'), ('source_id', 'H_Source')]
        
    def getDataType(self, attrName):
        if 'id' in attrName:
            return 'int'
        elif attrName == 'version':
            return 'int'
        elif 'timestamp' in attrName or 'date' in attrName:
            return 'datetime'
        else:
            return 'string'
    
    def create_headers(self):
        for key in self.attrmap:
            if key[:2] == 'L_':
                #skip links! These should be represented as relationships
                continue
            attrs = self.attrmap[key]
            schema_str = []
            for i,a in enumerate(attrs):
                if a == 'id':
                    schema_str.append(a + ':ID(' + key + '-ID)')
                else:
                    dtype = self.getDataType(a)
                    if dtype == 'int':
                        schema_str.append(a + ':int')
                    elif dtype == 'datetime':
                        schema_str.append(a + ':datetime')
                    else:
                        schema_str.append(a)
            
            with open(key + '-header.csv', 'w+') as fh:
                csvwriter = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csvwriter.writerow(schema_str)
    
    def create_link_headers(self):
        for key in self.linkmap:
            if key == 'L_AssetsInActions':
                for e in self.linkmap[key]:
                    st_node = e[0]
                    end_node = e[1]
                    st_fk = st_node[0]
                    st_fkname = st_node[1]
                    end_fk = end_node[0]
                    end_fkname = end_node[1]
                    attrs = self.attrmap[key]
                    schema_str = []
                    for i,a in enumerate(attrs):
                        if a == st_fk:
                            schema_str.append(':START_ID(' + st_fkname + '-ID)')
                        elif a == end_fk:
                            schema_str.append(':END_ID(' + end_fkname + '-ID)')
                        else:
                            dt = self.getDataType(a)
                            if dt == 'int':
                                schema_str.append(a + ':int')
                            elif dt == 'datetime':
                                schema_str.append(a + ':datetime')
                            else:
                                schema_str.append(a)
                    rel_name = 'Rel_' + st_fkname + '_' + end_fkname
                    self.link2rel[key].append(rel_name)
                    
                    with open(rel_name + '-header.csv', 'w+') as fh:
                        csvwriter = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                        csvwriter.writerow(schema_str)
            else:
                st_node = self.linkmap[key]
                end_node = self.linkmap[key]
                st_fk = st_node[0]
                st_fkname = st_node[1]
                end_fk = end_node[0]
                end_fkname = end_node[1]
                attrs = self.attrmap[key]
                schema_str = []
                for i,a in enumerate(attrs):
                    if a == st_fk:
                        schema_str.append(':START_ID(' + st_fkname + '-ID)')
                    elif a == end_fk:
                        schema_str.append(':END_ID(' + end_fkname + '-ID)')
                    else:
                        dt = self.getDataType(a)
                        if dt == 'int':
                            schema_str.append(a + ':int')
                        elif dt == 'datetime':
                            schema_str.append(a + ':datetime')
                        else:
                            schema_str.append(a)
                rel_name = 'Rel_' + st_fkname + '_' + end_fkname
                self.link2rel[key].append(rel_name)
                
                with open(rel_name + '-header.csv', 'w+') as fh:
                    csvwriter = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    csvwriter.writerow(schema_str)
                
    def create_relheaders(self):
        for key in self.fkmap:
            #create_link_headers already handles links
            if key[:2] == 'L_':
                continue
            for p in self.fkmap[key]:
                fk = p[0]
                fk_name = p[1]
                schema_str = []
                attrs = self.attrmap[key]
                for i,a in enumerate(attrs):
                    if a == 'id':
                        schema_str.append(':START_ID(' + key + '-ID)')
                    elif a == fk:
                        schema_str.append(':END_ID(' + fk_name + '-ID)')
                    else:
                        schema_str.append(a + ':IGNORE')
                
                rel_name = 'Rel_' + key + '_' + fk_name
                if fk == 'write_user_id':
                    rel_name = 'Rel_' + key + '_Write_' + fk_name 
                
                self.table2rel[key].append(rel_name)
                
                with open(rel_name + '-header.csv', 'w+') as fh:
                    csvwriter = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    csvwriter.writerow(schema_str)
    
    def create_bulk_command(self):
        table_order = ['H_UserType', 'H_User', 'H_AssetType',
                        'H_Asset', 'H_WhoProfile', 'H_WhatProfile',
                        'H_HowProfile', 'H_WhyProfile', 'H_WhenProfile',
                        'H_SourceType', 'H_Source', 'H_WhereProfile', 
                        'H_Action', 'H_RelationshipType', 'H_Relationship',
                        'L_UserTypeLink', 'L_AssetTypeLink', 'L_Asset_WhoProfile',
                        'L_WhoProfileUser', 'L_Asset_HowProfile', 'L_Asset_WhyProfile',
                       'L_Asset_WhatProfile', 'L_Asset_WhenProfile', 'L_Source2Type',
                        'L_Asset_WhereProfile', 'L_AssetsInActions', 'L_Relationship_Type',
                        'L_Asset_Relationships', 'S_User_schema', 'S_WhoProfile_schema',
                        'S_HowProfile_schema', 'S_WhyProfile_schema', 'S_WhatProfile_schema',
                        'S_WhenProfile_Attributes', 'S_Configuration', 'S_SourceTypeAttributes',
                        'S_AssetTypeAttributes', 'S_UserTypeAttributes',
                        'S_RelationshipTypeAttributes', 'S_Relationship_schema',
                        'S_Source_schema', 'L_WhereProfile_Source']
        
        res_str = 'bin/neo4j-admin import --database=datavault --skip-duplicate-nodes --skip-bad-relationships '
        res_str += '--id-type=INTEGER '
        
        for tname in table_order:
            #only hubs and satellites should be imported as nodes
            if tname[:2] == 'L_':
                continue
            res_str += '--nodes=' + tname + '=import/' + tname + '-header.csv,'
            res_str += 'import/' + tname + '.csv '
            
        for key in self.table2rel:
            for rel_name in self.table2rel[key]:
                res_str += '--relationships=' + rel_name + '=import/'
                res_str += rel_name + '-header.csv,import/' + key + '.csv '
        
        for key in self.link2rel:
            for rel_name in self.link2rel[key]:
                res_str += '--relationships=' + rel_name + '=import/'
                res_str += rel_name + '-header.csv,import/' + key + '.csv '
        
        with open('full_dv_loader.sh', 'w+') as fh:
            fh.write('#! /bin/sh\n')
            fh.write('BASEDIR=$(pwd)\n\n')
            fh.write(res_str)