
def vtexsqlscriptjson(schema):
    scripts ={ 'grafico1':f"""
                    SET CLIENT_ENCODING = 'UTF8';
                                                    
                    select 
                    cast(DATE_TRUNC('day',  ori.creationdate) as varchar(20))  as dategenerate,
                    cast(DATE_TRUNC('day',  ori.creationdate) as varchar(20))  as dategenerate2
                    from "{schema}".orders ori
                    limit 1;       
                """ ,

                 'grafico2':f"""
                    SET CLIENT_ENCODING = 'UTF8';
                                                    
                    select 
                    cast(DATE_TRUNC('day',  ori.creationdate) as varchar(20))  as dategenerate,
                    cast(DATE_TRUNC('day',  ori.creationdate) as varchar(20))  as dategenerate2
                    from "{schema}".orders ori
                    limit 1;       
                """ 
    
    }
    # Convertendo o dicionário para uma string JSON
  
    return scripts



# if __name__ == "__main__":
#     with open("Output.txt", "w") as text_file:
#         text_file.write(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))
#         print(vtexsqlscriptsorderslistupdate("6d41d249-d875-41ef-800e-eb0941f6d86f"))