# Cypher-and-Neo4j
loadCompany.py takes the data directory as a command line argument and creates a NEO4J database that stores all entities as nodes and all relationships as edges.

I have created the most popular Company database in noe4j using dummy data. The following labels for nodes and relationships are created

Node Labels:
  
"Department"
"Employee"
"Dependent"
"Project"

Relationship Labels:

Relationship(d,"employs",e)
Relationship(e,"works_for",d)

Relationship(e,"supervisee",boss)
Relationship(boss,"supervisor",e)

Relationship(d,"managed_by",e)
Relationship(e,"manages",d)

Relationship(e,"dependent",d)
Relationship(d,"dependent_of",e)

Relationship(d,"controls",p)
Relationship(p,"controlled_by",d)

Relationship(p,"worker",e)
Relationship(e,"works_on",p)

company.py is a Python-Flask RESTful services that implements the following endpoints that could be used by a UI frontend (index.html) to browse the data in the entire database:

@app.route('/company/departments/', methods=['GET'])
@app.route('/company/employees/', methods=['GET'])
@app.route('/company/projects/', methods=['GET'])
@app.route('/company/cities/', methods=['GET'])
@app.route('/company/supervisees/<string:ssn>/', methods=['GET'])
@app.route('/company/department/<int:dno>/', methods=['GET'])
@app.route('/company/employee/<string:ssn>/', methods=['GET'])
@app.route('/company/project/<int:pno>/', methods=['GET'])
@app.route('/company/projects/<string:cty>/', methods=['GET'])
@app.route('/company/departments/<string:cty>/', methods=['GET'])
