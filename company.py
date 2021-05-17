from flask import Flask, jsonify
from py2neo import Graph
from flask_cors import CORS
app = Flask(__name__)
CORS(app)
g = Graph(auth=('neo4j', 'r123'))


@app.route('/company/departments/', methods=['GET'])
def get_departments():
    query = g.run(
    """
    MATCH (d:Department)
    RETURN d.dno as departments
    """)
    res={}
    data=[]
    for row in query.data():
            data.append(int(row['departments']))
    res['departments']=sorted(data)
    return res
@app.route('/company/projects/', methods=['GET'])
def get_projects():
    query = g.run(
    """
    MATCH (p:Project)
    RETURN DISTINCT p.pno as projects
    """)
    res={}
    data=[]
    for row in query.data():
            data.append(int(row['projects']))
    res['projects']=sorted(data)
    return res

@app.route('/company/employees/', methods=['GET'])
def get_employees():
    query = g.run(
"""
MATCH (e:Employee)
RETURN DISTINCT e.essn as employees
""")
    res={}
    data=[]
    for row in query.data():
            data.append(row['employees'])
    res['employees']=sorted(data)
    return res

@app.route('/company/department/<int:dno>/', methods=['GET'])
def get_department_info(dno):
    query1 = g.run("""
        MATCH (d:Department{dno:$dno})-[r:controls]->(p:Project)
        RETURN p.pname as pname, p.pno as pnumber
        """, dno=str(dno))

    query2=g.run("""
    MATCH (d:Department{dno:$dno})
    RETURN d.dname as dept_name, d.locations as locations
    """,dno=str(dno))

    query3=g.run("""
    MATCH (e:Employee)-[r:manages]->(d:Department{dno:$dno})
    RETURN e.fname as fname, e.lname as lname, e.essn as manager_ssn, d.sdate as manager_start_date
    """,dno=str(dno))

    query4=g.run("""
    MATCH (d:Department{dno:$dno})-[r:employs]->(e:Employee)
    RETURN DISTINCT e.essn as emp_ssn
    """,dno=str(dno))
    res={}
    controlled_projects, employees, locations =[] , [], []
    loc, dname, manager, manager_start_date, mgrssn = '','','','',''

    for row in query1.data():
        controlled_projects.append({"pname":row['pname'],"pnumber":int(row['pnumber'])})
    for row in query2.data():
        locations=row['locations']
        dname=row['dept_name']

    for row in query3.data():
        manager=row['fname']+" "+row['lname']
        manager_start_date=row['manager_start_date']
        mgrssn=row['manager_ssn']
    for row in query4.data():
        employees.append(row['emp_ssn'])

    res['controlled_projects'] = controlled_projects
    res['dname']=dname
    res['employees']=employees
    res['locations']=locations
    res['manager']=manager
    res['manager_start_date']=manager_start_date
    res['mgrssn']=mgrssn
    return res

@app.route('/company/project/<int:pno>/', methods=['GET'])
def get_project_info(pno):
    query1 = g.run("""MATCH (p:Project{pno:$pno})-[r:controlled_by]->(d:Department)
                        RETURN p.pname as pname, d.dname as controlling_dname, d.dno as controlling_dnumber, d.locations as locations""",
                   pno=str(pno))

    query2 = g.run("""MATCH (e:Employee)-[r:works_on]->(p{pno:$pno})
                        RETURN e.essn as employees, e.dno as dept_number, r.Hours as hours_each
                        """, pno=str(pno))
    res, dept_hrs = {}, {}
    employees = []
    controlling_dname, controlling_dname, locations = '', '', ''
    hours = 0
    for row in query1.data():
        controlling_dname = row['controlling_dname']
        controlling_dnumber = row['controlling_dnumber']
        pname = row['pname']
        locations = row['locations']
    data1 = []
    for row in query2.data():
        employees.append(row['employees'])
        if row['hours_each'] != None:
            hours = hours + float(row['hours_each'])
            data1.append([row['employees'], float(row['hours_each'])])
    res['controlling_dname'] = controlling_dname
    res['controlling_dnumber'] = controlling_dnumber
    res['pname'] = pname
    res['employees'] = employees
    res['person_hours'] = hours
    res['locations'] = locations
    dept = []
    dept_hrs
    for e in employees:
        k = g.run("""MATCH (e:Employee{essn:$ssn})-[r:works_For]->(d)
                        RETURN e.essn as employees, e.dno as dept_number, d.dname as dname
                        """, ssn=str(e))
        for i in k.data():
            dept.append([i["employees"], i["dept_number"], i["dname"]])
    # adjusting dept hours
    for i in dept:
        for j in data1:
            if i[0] in j:
                i.append(j[1])

    for i in dept:
        if i[2] not in dept_hrs:
            dept_hrs[i[2]] = float(i[3])
        else:
            dept_hrs[i[2]] = dept_hrs[i[2]] + float(i[3])

    res['dept_hours'] = dept_hrs
    return res

@app.route('/company/employee/<string:ssn>/', methods=['GET'])
def get_employee_info(ssn):
    query1 = g.run("""
                MATCH (e:Employee{essn:$ssn})-[r:dependent]->(d:Dependent)
                RETURN d.name as dname,d.dob as bdate, d.sex as gender,d.relation as relationship
                """, ssn=str(ssn))
    query2 = g.run("""
                MATCH (e:Employee{essn:$ssn})-[r:works_on]->(p:Project)
                RETURN p.pname as pname, p.pno as pnumber, r.Hours as hours
                """, ssn=str(ssn))

    query3 = g.run("""
                MATCH (e:Employee{sssn:$ssn})
                RETURN e.essn as supervisees
                """, ssn=str(ssn))
    query4 = g.run("""
                MATCH (e{essn:$ssn})-[r:manages]->(d:Department)
                RETURN d.dname as dname,d.dno as dnumber
                """, ssn=str(ssn))
    query5 = g.run("""
                MATCH (e:Employee{essn:$ssn})
                RETURN e.add as address,e.dob as bdate,e.lname as lname,e.fname as fname,e.sex as gender,e.minit as minit,e.salary as salary,e.sssn as supervisor
                """, ssn=str(ssn))
    query6 = g.run("""
                MATCH (e:Employee{essn:$ssn})-[r:works_for]->(d:Department)
                RETURN d.dname as department_name, d.dno as department_number
                """, ssn=str(ssn))
    res, manages = {},{}
    dependents, projects, supervisees = [], [], []

    for row in query1.data():
        dependents.append({"dname": row['dname'], "bdate": row['bdate'], "gender": row['gender'],
                           "relationship": row['relationship']})
    for row in query2.data():
        projects.append({"pname": row['pname'], "pnumber": int(row['pnumber']), "hours": row['hours']})
    for row in query3.data():
        supervisees.append(row['supervisees'])
    for row in query4.data():
        manages["dname"] = row['dname']
        manages["dnumber"] = row['dnumber']
    for row in query5.data():
        res["address"] = row['address']
        res["bdate"] = row['bdate']
        res["lname"] = row['lname']
        res["fname"] = row['fname']
        res["gender"] = row['gender']
        res["minit"] = row['minit']
        res["salary"] = row['salary']
        res["supervisor"] = row['supervisor']
    for row in query6.data():
        res["department_name"] = row['department_name']
        res["department_number"] = int(row['department_number'])
    res['dependents'] = dependents
    res['projects'] = projects
    res['manages'] = manages
    res['supervisees'] = supervisees
    return res

@app.route('/company/supervisees/<string:ssn>/', methods=['GET'])
def get_supervisees_info(ssn):
    query = g.run("""
            MATCH (e:Employee{sssn:$ssn})
            RETURN DISTINCT e.essn as employee
            """, ssn=str(ssn))
    res = {}
    employees = []
    for row in query.data():
        employees.append(row['employee'])
    res['employees'] = employees
    return res

@app.route('/company/cities/', methods=['GET'])
def get_cities():
    query1 = g.run(
        """
        MATCH (d:Department)
        RETURN d.locations as cities
        """)
    query2 = g.run(
        """
        MATCH (p:Project)
        RETURN p.ploc as cities
        """)

    res = {}
    city = []
    for row in query1.data():
        for c in row["cities"]:
            city.append(c)
    for row in query2.data():
        city.append(row['cities'])
    c = set(city)
    res['cities'] = c
    return res

@app.route('/company/projects/<string:cty>/', methods=['GET'])
def project_by_city(cty):
    query1 = g.run(
        """
        MATCH (p:Project{ploc:$city})
        RETURN p.pname as pname, p.pno as pnumber
        """, city=str(cty))
    res = {}
    projects = []
    for row in query1.data():
        project = {}
        project["pname"] = row["pname"]
        project["pnumber"] = row["pnumber"]
        projects.append(project)
    res['projects'] = projects
    return res

@app.route('/company/departments/<string:cty>/', methods=['GET'])
def dept_by_city(cty):
    query1 = g.run(
        """
        MATCH (d:Department)
        RETURN d.dname as dname, d.dno as dnumber, d.locations as locations
        """, city=str(cty))
    res = {}
    departments = []
    for row in query1.data():
        if cty in row["locations"]:
            dept = {}
            dept["dname"] = row["dname"]
            dept["dnumber"] = row["dnumber"]
            departments.append(dept)
    res['departments'] = departments
    return res


if __name__ == "__main__":
   app.run(host='localhost',port= '5000', debug=True)
