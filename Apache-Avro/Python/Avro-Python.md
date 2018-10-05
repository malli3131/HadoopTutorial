## Python Avro Example:

#### Install Python Avro Client:

```
  1. Download the Avro Python Client from Apache Website:
  http://mirrors.wuchna.com/apachemirror/avro/avro-1.8.2/py/avro-1.8.2.tar.gz
  
  2. Extract the tar from and install Avro Python client
     tar -xvf avro-1.8.2.tar.gz
     cd avro-1.8.2
     sudo python setup.py install
     check the avro python client installed or not?
     > python
     >>> import avro
     No import error then Avro Python client is installed successfully!
     
  3. Create the Avro schema for sample message:
      
      {"namespace":"example.avro",
      "type":"record",
      "name":"User",
      "fields":[
	          {"name":"name", "type":"string"},
	          {"name":"age", "type":"int"},
	          {"name":"place", "type":"string"}
        ]
       }
       
       Save the file with name user.avsc in Specified path say /home/$USER/user.avsc
       
    4. Write the sample data into a file called user.avro file.
    
        import avro
        import avro.schema
        from avro.datafile import DataFileReader, DataFileWriter
        from avro.io import DatumReader, DatumWriter
        
        schema = avro.schema.parse(open("/path/to/user.avsc", "rb").read())
        data = DataFileWriter(open("/path/to/user.avro", "wb"), DatumWriter(), schema)
        
        data.append({"name":"Naga", "age":30, "place":"Bangalore"})
        data.append({"name":"Ravi", "age":33, "place":"Bangalore"})
        data.append({"name":"Hari", "age":36, "place":"Mangalore"})
        data.append({"name":"Siva", "age":26, "place":"Mysore"})
        data.flush()
        
        This will successfully write sample data records into user.avro file.
        
    5.  Read the user.avro file and print the data.
    
        mydata = DataFileReader(open("/path/to/user.avro", "rb"), DatumReader)
        
        for user in mydata:                                                                      
            print user
            
        Result:
          {u'age': 30, u'place': u'Bangalore', u'name': u'Naga'}
          {u'age': 33, u'place': u'Bangalore', u'name': u'Ravi'}
          {u'age': 36, u'place': u'Mangalore', u'name': u'Hari'}
          {u'age': 26, u'place': u'Mysore', u'name': u'Siva'}

```
