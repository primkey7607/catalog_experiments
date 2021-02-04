import csv

class NormImportCreator:
    
    def __init__(self):
        self.attrmap = {}
        self.init_attrmap()
        self.fkmap = {}
        self.init_fkmap()
    
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
    #keys: table names
    #values: FKs of the table
    def init_fkmap(self):
        self.fkmap['User'] = [('user_type_id', 'UserType'), ('user_id', 'User')]
        self.fkmap['Asset'] = [('asset_type_id', 'AssetType'), ('user_id', 'User')]
        self.fkmap['WhoProfile'] = [('write_user_id', 'User'), ('asset_id', 'Asset'),
                                    ('user_id', 'User')]
        self.fkmap['WhatProfile'] = [('asset_id', 'Asset'), ('user_id', 'User')]
        self.fkmap['HowProfile'] = [('asset_id', 'Asset'), ('user_id', 'User')]
        self.fkmap['WhyProfile'] = [('asset_id', 'Asset'), ('user_id', 'User')]
        self.fkmap['WhenProfile'] = [('asset_id', 'Asset'), ('user_id', 'User')]
        self.fkmap['Source'] = [('user_id', 'User'), ('source_type_id', 'SourceType')]
        self.fkmap['WhereProfile'] = [('asset_id', 'Asset'), ('user_id', 'User'),
                                      ('source_id', 'Source')]
        self.fkmap['Relationship'] = [('user_id', 'User'), ('relationship_type_id', 'RelationshipType')]
        self.fkmap['Asset_Relationships'] = [('asset_id', 'Asset'), ('relationship_id', 'Relationship')]
        self.fkmap['Action'] = [('user_id', 'User'), ('asset_id', 'Asset'),
                                ('who_id', 'WhoProfile'), ('how_id', 'HowProfile'),
                                ('why_id', 'WhyProfile'), ('when_id', 'WhenProfile')]
    
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
    
    def create_relheaders(self):
        for key in self.fkmap:
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
                
                with open('Rel_' + key + '_' + fk_name + '-header.csv', 'w+') as fh:
                    csvwriter = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    csvwriter.writerow(schema_str)
                    
    
    
    
    def create_bulk_command(self):
        table_order = ['UserType', 'User', 'AssetType',
                                    'Asset', 'WhoProfile', 'WhatProfile',
                                    'HowProfile','WhyProfile', 'WhenProfile',
                                    'SourceType', 'Source', 'WhereProfile', 
                                    'Action', 'RelationshipType', 'Relationship',
                                    'Asset_Relationships']
        
        res_str = 'bin/neo4j-admin import --database=normalizedv8 --skip-duplicate-nodes --skip-bad-relationships '
        res_str += '--id-type=INTEGER '
        for tname in table_order:
            res_str += '--nodes=' + tname + '=import/' + tname + '-header.csv,'
            res_str += 'import/' + tname + '.csv '
        
        #now, add relationships
        for key in self.fkmap:
            for p in self.fkmap[key]:
                fk_name = p[1]
                rel_name = 'Rel_' + key + '_' + fk_name
                res_str += '--relationships=' + rel_name + '=import/'
                res_str += rel_name + '-header.csv,import/' + key + '.csv '
        
        with open('full_norm_loader.sh', 'w+') as fh:
            fh.write('#! /bin/sh\n')
            fh.write('BASEDIR=$(pwd)\n\n')
            fh.write(res_str)
            
                

if __name__ == "__main__":
    nic = NormImportCreator()
    nic.create_headers()
    nic.create_relheaders()
    nic.create_bulk_command()
                    
            
        
