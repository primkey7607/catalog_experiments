from neo4j import GraphDatabase
import os

class GenNNeo4j:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.attrmap = {}
        self.init_attrmap()
        self.tableLst = ['UserType', 'User', 'AssetType',
                                    'Asset', 'WhoProfile', 'WhatProfile',
                                    'HowProfile', 'WhyProfile', 'WhenProfile',
                                    'SourceType', 'Source', 'WhereProfile', 
                                    'Action', 'RelationshipType', 'Relationship',
                                    'Asset_Relationships']
    
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
    
    def load_table(self, tx, tname):
        #LOAD CSV FROM '{csv-dir}/artists.csv' AS line
        #CREATE (:Artist { name: line[1], year: toInteger(line[2])})
        attrs = self.attrmap[tname]
        query_str = "LOAD CSV FROM \'file:///home/pranav/catalog_experiments/" + tname + ".csv\' AS line "
        query_str += "CREATE ( :" + tname + " { "
        for i,a in enumerate(attrs):
            dtype = self.getDataType(a)
            type_str = ""
            type_str1 = ''
            type_str2 = ''
            if dtype == 'int':
                type_str = 'toInteger'
            elif dtype == 'datetime':
                #type_str = 'toDateTime'
                #type_str = 'date'
                type_str = 'datetime'
                type_str1 = 'datetime({epochMillis:apoc.date.parse('
                type_str2 = ', \'ms\', \'yyyy-MM-dd HH:mm:ss\')})'
            
            if dtype == 'datetime':
                query_str += a + ": " + type_str1 + 'line[' + str(i) + ']' + type_str2 + ', '
            elif type_str == "":
                query_str += a + ": " + "line[" + str(i) + "], "
            else:
                query_str += a + ": " + type_str + "(line[" + str(i) + "]), "
        
        query_str = query_str[:-2] + " })"
        
        result = tx.run(query_str)
        #return result.single()[0]
        return result.single()
    
    def load_all_tables(self):
        with self.driver.session() as session:
            for tname in self.tableLst:
                res_msg = session.write_transaction(self.load_table, tname)
                print(res_msg)
    
    def create_relquery(self, t1, t2, k1, k2):
        #sample: MATCH (a:Person),(b:Person)
        #WHERE a.name = 'A' AND b.name = 'B'
        #CREATE (a)-[r:RELTYPE]->(b)
        #RETURN type(r)
        query_str = "MATCH (a:" + t1 + "), (b:" + t2 + ") "
        query_str += "WHERE a." + k1 + " = b." + k2
        query_str += " CREATE (a)-[r:RELTYPE]->(b);"
        return query_str
    
    def load_relationship(self, tx, tname):
        if tname == 'User':
            query_str1 = self.create_relquery('User', 'UserType', 'user_type_id', 'id')
            query_str2 = self.create_relquery('User', 'User', 'user_id', 'id')
            print(query_str1)
            print(query_str2)
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            #print(result1.peek())
            #print(result2.peek())
            return result2.peek()
        elif tname == 'Asset':
            query_str1 = self.create_relquery('Asset', 'AssetType', 'asset_type_id', 'id')
            query_str2 = self.create_relquery('Asset', 'User', 'user_id', 'id')
            print(query_str1)
            print(query_str2)
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            #print(result1.peek())
            #print(result2.peek())
            return result2.peek()
        elif tname == 'WhoProfile':
            #['id', 'version', 'timestamp', 'write_user_id',
            #'asset_id', 'user_id', 'schema']
            query_str1 = self.create_relquery('WhoProfile', 'User', 'write_user_id', 'id')
            query_str2 = self.create_relquery('WhoProfile', 'User', 'user_id', 'id')
            query_str3 = self.create_relquery('WhoProfile', 'Asset', 'asset_id', 'id')
            print(query_str1)
            print(query_str2)
            print(query_str3)
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            result3 = tx.run(query_str3)
            #print(result1.peek())
            #print(result2.peek())
            #print(result3.peek())
            return result3.peek()
        elif tname == 'WhatProfile':
            #['id', 'version', 'timestamp', 'user_id',
            #'asset_id', 'schema']
            query_str1 = self.create_relquery('WhatProfile', 'User', 'user_id', 'id')
            query_str2 = self.create_relquery('WhatProfile', 'Asset', 'asset_id', 'id')
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            print(query_str1)
            print(query_str2)
            #print(result1.peek())
            #print(result2.peek())
            return result2.peek()
        elif tname == 'HowProfile':
            #['id', 'version', 'timestamp', 'user_id',
            #'asset_id', 'schema']
            query_str1 = self.create_relquery('HowProfile', 'User', 'user_id', 'id')
            query_str2 = self.create_relquery('HowProfile', 'Asset', 'asset_id', 'id')
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            print(query_str1)
            print(query_str2)
            #print(result1.peek())
            #print(result2.peek())
            return result2.peek()
        elif tname == 'WhyProfile':
            query_str1 = self.create_relquery('WhyProfile', 'User', 'user_id', 'id')
            query_str2 = self.create_relquery('WhyProfile', 'Asset', 'asset_id', 'id')
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            print(query_str1)
            print(query_str2)
            #print(result1.peek())
            #print(result2.peek())
            return result2.peek()
        elif tname == 'WhenProfile':
            #['id', 'version', 'timestamp', 'user_id',
            #'asset_id', 'asset_timestamp',
            #'expiry_date', 'start_date']
            query_str1 = self.create_relquery('WhenProfile', 'User', 'user_id', 'id')
            query_str2 = self.create_relquery('WhenProfile', 'Asset', 'asset_id', 'id')
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            print(query_str1)
            print(query_str2)
            #print(result1.peek())
            #print(result2.peek())
            return result2.peek()
        elif tname == 'Source':
            #['id', 'version', 'timestamp', 'user_id',
            #'name', 'source_type_id', 'schema']
            query_str1 = self.create_relquery('Source', 'User', 'user_id', 'id')
            query_str2 = self.create_relquery('Source', 'SourceType', 'source_type_id', 'id')
            print(query_str1)
            print(query_str2)
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            #print(result1.peek())
            #print(result2.peek())
            return result2.peek()
        elif tname == 'WhereProfile':
            #['id', 'version', 'timestamp', 'user_id',
            #'asset_id', 'access_path', 'source_id', 'configuration']
            query_str1 = self.create_relquery('WhereProfile', 'User', 'user_id', 'id')
            query_str2 = self.create_relquery('WhereProfile', 'Asset', 'asset_id', 'id')
            query_str3 = self.create_relquery('WhereProfile', 'Source', 'source_id', 'id')
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            result3 = tx.run(query_str3)
            print(query_str1)
            print(query_str2)
            print(query_str3)
            #print(result1.peek())
            #print(result2.peek())
            #print(result3.peek())
            return result3.peek()
        elif tname == 'Relationship':
            #['id', 'version', 'timestamp', 'user_id',
            #'relationship_type_id', 'schema']
            query_str1 = self.create_relquery('Relationship', 'User', 'user_id', 'id')
            query_str2 = self.create_relquery('Relationship', 'RelationshipType', 'relationship_type_id', 'id')
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            print(query_str1)
            print(query_str2)
            #print(result1.peek())
            #print(result2.peek())
            return result2.peek()
        elif tname == 'Asset_Relationships':
            #['id', 'asset_id', 'relationship_id']
            query_str1 = self.create_relquery('Asset_Relationships', 'Asset', 'asset_id', 'id')
            query_str2 = self.create_relquery('Asset_Relationships', 'Relationship', 'relationship_id', 'id')
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            print(query_str1)
            print(query_str2)
            #print(result1.peek())
            #print(result2.peek())
            return result2.peek()
        elif tname == 'Action':
            #['id', 'version', 'timestamp', 'user_id',
            #'asset_id', 'who_id', 'how_id', 'why_id',
            #'when_id']
            query_str1 = self.create_relquery('Action', 'User', 'user_id', 'id')
            query_str2 = self.create_relquery('Action', 'Asset', 'asset_id', 'id')
            query_str3 = self.create_relquery('Action', 'WhoProfile', 'who_id', 'id')
            query_str4 = self.create_relquery('Action', 'WhoProfile', 'who_id', 'id')
            query_str5 = self.create_relquery('Action', 'HowProfile', 'how_id', 'id')
            query_str6 = self.create_relquery('Action', 'WhyProfile', 'why_id', 'id')
            query_str7 = self.create_relquery('Action', 'WhenProfile', 'when_id', 'id')
            print(query_str1)
            print(query_str2)
            print(query_str3)
            print(query_str4)
            print(query_str5)
            print(query_str6)
            print(query_str7)
            result1 = tx.run(query_str1)
            result2 = tx.run(query_str2)
            result3 = tx.run(query_str3)
            result4 = tx.run(query_str4)
            result5 = tx.run(query_str5)
            result6 = tx.run(query_str6)
            result7 = tx.run(query_str7)
            #print(result1.peek())
            #print(result2.peek())
            #print(result3.peek())
            #print(result4.peek())
            #print(result5.peek())
            #print(result6.peek())
            #print(result7.peek())
            return result7.peek()
            
    def load_all_relationships(self):
        batch = 1000
        for tname in self.tableLst:
            if 'Type' in tname:
                   continue
            print("Loading Relationships for Table: " + tname)
            with self.driver.session() as session:
                query_str1 = self.create_relquery('User', 'UserType', 'user_type_id', 'id')
                query_str2 = self.create_relquery('User', 'User', 'user_id', 'id')
                print(query_str1)
                print(query_str2)
                session.run(query_str1, batch=batch)
                session.run(query_str2, batch=batch)
                query_str1 = self.create_relquery('Asset', 'AssetType', 'asset_type_id', 'id')
                query_str2 = self.create_relquery('Asset', 'User', 'user_id', 'id')
                print(query_str1)
                print(query_str2)
                session.run(query_str1, batch=batch)
                session.run(query_str2, batch=batch)
                query_str1 = self.create_relquery('WhoProfile', 'User', 'write_user_id', 'id')
                query_str2 = self.create_relquery('WhoProfile', 'User', 'user_id', 'id')
                query_str3 = self.create_relquery('WhoProfile', 'Asset', 'asset_id', 'id')
                print(query_str1)
                print(query_str2)
                print(query_str3)
                session.run(query_str1, batch=batch)
                session.run(query_str2, batch=batch)
                session.run(query_str3, batch=batch)
                query_str1 = self.create_relquery('WhatProfile', 'User', 'user_id', 'id')
                query_str2 = self.create_relquery('WhatProfile', 'Asset', 'asset_id', 'id')
                print(query_str1)
                print(query_str2)
                session.run(query_str1, batch=batch)
                session.run(query_str2, batch=batch)
                query_str1 = self.create_relquery('HowProfile', 'User', 'user_id', 'id')
                query_str2 = self.create_relquery('HowProfile', 'Asset', 'asset_id', 'id')
                print(query_str1)
                print(query_str2)
                session.run(query_str1, batch=batch)
                session.run(query_str2, batch=batch)
                
        # elif tname == 'HowProfile':
        #     #['id', 'version', 'timestamp', 'user_id',
        #     #'asset_id', 'schema']
        #     query_str1 = self.create_relquery('HowProfile', 'User', 'user_id', 'id')
        #     query_str2 = self.create_relquery('HowProfile', 'Asset', 'asset_id', 'id')
        #     result1 = tx.run(query_str1)
        #     result2 = tx.run(query_str2)
        #     print(query_str1)
        #     print(query_str2)
        #     #print(result1.peek())
        #     #print(result2.peek())
        #     return result2.peek()
        # elif tname == 'WhyProfile':
        #     query_str1 = self.create_relquery('WhyProfile', 'User', 'user_id', 'id')
        #     query_str2 = self.create_relquery('WhyProfile', 'Asset', 'asset_id', 'id')
        #     result1 = tx.run(query_str1)
        #     result2 = tx.run(query_str2)
        #     print(query_str1)
        #     print(query_str2)
        #     #print(result1.peek())
        #     #print(result2.peek())
        #     return result2.peek()
        # elif tname == 'WhenProfile':
        #     #['id', 'version', 'timestamp', 'user_id',
        #     #'asset_id', 'asset_timestamp',
        #     #'expiry_date', 'start_date']
        #     query_str1 = self.create_relquery('WhenProfile', 'User', 'user_id', 'id')
        #     query_str2 = self.create_relquery('WhenProfile', 'Asset', 'asset_id', 'id')
        #     result1 = tx.run(query_str1)
        #     result2 = tx.run(query_str2)
        #     print(query_str1)
        #     print(query_str2)
        #     #print(result1.peek())
        #     #print(result2.peek())
        #     return result2.peek()
        # elif tname == 'Source':
        #     #['id', 'version', 'timestamp', 'user_id',
        #     #'name', 'source_type_id', 'schema']
        #     query_str1 = self.create_relquery('Source', 'User', 'user_id', 'id')
        #     query_str2 = self.create_relquery('Source', 'SourceType', 'source_type_id', 'id')
        #     print(query_str1)
        #     print(query_str2)
        #     result1 = tx.run(query_str1)
        #     result2 = tx.run(query_str2)
        #     #print(result1.peek())
        #     #print(result2.peek())
        #     return result2.peek()
        # elif tname == 'WhereProfile':
        #     #['id', 'version', 'timestamp', 'user_id',
        #     #'asset_id', 'access_path', 'source_id', 'configuration']
        #     query_str1 = self.create_relquery('WhereProfile', 'User', 'user_id', 'id')
        #     query_str2 = self.create_relquery('WhereProfile', 'Asset', 'asset_id', 'id')
        #     query_str3 = self.create_relquery('WhereProfile', 'Source', 'source_id', 'id')
        #     result1 = tx.run(query_str1)
        #     result2 = tx.run(query_str2)
        #     result3 = tx.run(query_str3)
        #     print(query_str1)
        #     print(query_str2)
        #     print(query_str3)
        #     #print(result1.peek())
        #     #print(result2.peek())
        #     #print(result3.peek())
        #     return result3.peek()
        # elif tname == 'Relationship':
        #     #['id', 'version', 'timestamp', 'user_id',
        #     #'relationship_type_id', 'schema']
        #     query_str1 = self.create_relquery('Relationship', 'User', 'user_id', 'id')
        #     query_str2 = self.create_relquery('Relationship', 'RelationshipType', 'relationship_type_id', 'id')
        #     result1 = tx.run(query_str1)
        #     result2 = tx.run(query_str2)
        #     print(query_str1)
        #     print(query_str2)
        #     #print(result1.peek())
        #     #print(result2.peek())
        #     return result2.peek()
        # elif tname == 'Asset_Relationships':
        #     #['id', 'asset_id', 'relationship_id']
        #     query_str1 = self.create_relquery('Asset_Relationships', 'Asset', 'asset_id', 'id')
        #     query_str2 = self.create_relquery('Asset_Relationships', 'Relationship', 'relationship_id', 'id')
        #     result1 = tx.run(query_str1)
        #     result2 = tx.run(query_str2)
        #     print(query_str1)
        #     print(query_str2)
        #     #print(result1.peek())
        #     #print(result2.peek())
        #     return result2.peek()
        # elif tname == 'Action':
        #     #['id', 'version', 'timestamp', 'user_id',
        #     #'asset_id', 'who_id', 'how_id', 'why_id',
        #     #'when_id']
        #     query_str1 = self.create_relquery('Action', 'User', 'user_id', 'id')
        #     query_str2 = self.create_relquery('Action', 'Asset', 'asset_id', 'id')
        #     query_str3 = self.create_relquery('Action', 'WhoProfile', 'who_id', 'id')
        #     query_str4 = self.create_relquery('Action', 'WhoProfile', 'who_id', 'id')
        #     query_str5 = self.create_relquery('Action', 'HowProfile', 'how_id', 'id')
        #     query_str6 = self.create_relquery('Action', 'WhyProfile', 'why_id', 'id')
        #     query_str7 = self.create_relquery('Action', 'WhenProfile', 'when_id', 'id')
        #     print(query_str1)
        #     print(query_str2)
        #     print(query_str3)
        #     print(query_str4)
        #     print(query_str5)
        #     print(query_str6)
        #     print(query_str7)
        #     result1 = tx.run(query_str1)
        #     result2 = tx.run(query_str2)
        #     result3 = tx.run(query_str3)
        #     result4 = tx.run(query_str4)
        #     result5 = tx.run(query_str5)
        #     result6 = tx.run(query_str6)
        #     result7 = tx.run(query_str7)

    def load_one(self, tname):
        with self.driver.session() as session:
            res_msg = session.write_transaction(self.load_table, tname)
            print(res_msg)
        
    def close(self):
        self.driver.close()

if __name__ == "__main__":
    graph_loader = GenNNeo4j("bolt://localhost:7687", "neo4j", "normal")
    #graph_loader.load_one("HowProfile")
    graph_loader.load_all_tables()
    graph_loader.load_all_relationships()
    graph_loader.close() 

