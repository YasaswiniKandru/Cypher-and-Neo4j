import sys
import csv
from py2neo import Graph, Node, Relationship, NodeMatcher

def loadDepartments(g,fname):
  with open(fname, 'r') as f:
    rows = list(csv.reader(f, delimiter="\n"))
  for row in rows:
    r = row[0].split(':')
    n = Node("Department", dname=r[0], dno=r[1], sssn=r[2], sdate=r[3])
    g.create(n)

def loadDependents(g,fname):
  with open(fname, 'r') as f:
    rows = list(csv.reader(f, delimiter="\n"))
  for row in rows:
    r = row[0].split(':')
    n = Node("Dependent", essn=r[0], name=r[1], sex=r[2], dob=r[3], relation=r[4])
    g.create(n)

def loadEmployees(g,fname):
  with open(fname, 'r') as f:
    rows = list(csv.reader(f, delimiter="\n"))
  for row in rows:
    r = row[0].split(':')
    n = Node("Employee", fname=r[0], minit=r[1], lname=r[2], essn=r[3], dob=r[4], add=r[5], sex=r[6], salary=r[7], sssn=r[8], dno=r[9])
    g.create(n)


def loadProjects(g,fname):
  with open(fname, 'r') as f:
    rows = list(csv.reader(f, delimiter="\n"))
  for row in rows:
    r = row[0].split(':')
    n = Node("Project", pname=r[0], pno=r[1], ploc=r[2], dno=r[3])
    g.create(n)

def RelationEmploys(g):
    query = """
       MATCH (d:Department), (e:Employee)
       WHERE d.dno=e.dno 
       CREATE (d)-[:employs]->(e)
       CREATE (d)-[:managed_by]->(e)
      """
    g.run(query)


def RelationWorks_For(g):
  query = """
       MATCH (d:Department), (e:Employee) 
       WHERE e.dno=d.dno 
       CREATE (e)-[:works_For]->(d)
       CREATE (e)-[:manages]->(d)
      """
  g.run(query)

def RelationDependent(g):  #Did not get this
  query = """
       MATCH (d:Dependent), (e:Employee) 
       WHERE e.essn=d.essn 
       CREATE (e)-[:dependent]->(d)
     """
  g.run(query)

def RelationDependent_of(g):
  query = """
       MATCH (d:Dependent), (e:Employee) 
       WHERE d.essn=e.essn 
       CREATE (d)-[:dependent_of]->(e)
     """
  g.run(query)

def RelationControls(g):
  query = """
       MATCH (d:Department), (p:Project) 
       WHERE d.dno=p.dno 
       CREATE (d)-[:controls]->(p)
     """
  g.run(query)

def RelationControlled_by(g):  #Did not get this
  query = """
       MATCH (d:Department), (p:Project) 
       WHERE p.dno=d.dno
       CREATE (p)-[:controlled_by]->(d)
     """
  g.run(query)

def Loadworks_on(g, fname):
  print("lload workson")
  with open(fname, 'r') as f:
    rows = list(csv.reader(f, delimiter="\n"))
  query = """
           MATCH (e:Employee{essn:$eno}), (p:Project{pno:$pno}) 
           MERGE (e)-[r:works_on {Hours: $hrs}]->(p)
           MERGE (p)-[r1:worker {Hours: $hrs}]->(e)
         """
  for row in rows:
    r = row[0].split(':')
    print(r[0])
    print(r[1])
    print(r[2])

    x= g.run(query, eno=str(r[0]), pno=str(r[1]), hrs=str(r[2]))
    print(x)

def RelationSupervisor(g):
  query = """ 
    MATCH (e:Employee),(s:Employee)
    WHERE e.sssn = s.essn
    CREATE (s)-[r:supervisor]->(e)
    RETURN r"""
  x=g.run(query)
  print("supervisor")
  print(x)

def RelationSupervisee(g):
  query = """ 
    MATCH (e:Employee),(s:Employee)
    WHERE e.sssn = s.essn
    CREATE (e)-[r:supervisee]->(s)
    RETURN r"""
  g.run(query)

def addlocations(g,fname):
    with open(fname, 'r') as f:
        rows = list(csv.reader(f, delimiter="\n"))
    for row in rows:
        r = row[0].split(':')
        id = r[0]
        values =[]
        for i in r[1:]:
            values.append(str(i))
        query =""" 
        MATCH (n:Department)
        WHERE n.dno = $id
        SET n.locations = $values
        RETURN n
        """
        x = g.run(query, id =id, values = values)

if __name__ == '__main__':
  g = Graph(auth=('neo4j','r123'))
  g.delete_all()
  loadDepartments(g, "./" + sys.argv[1] + "/DEPARTMENTS.dat")
  loadDependents(g, "./" + sys.argv[1] + "/DEPENDENTS.dat")
  loadEmployees(g, "./" + sys.argv[1] + "/EMPLOYEES.dat")
  loadProjects(g,"./" + sys.argv[1]+"/PROJECTS.dat")
  RelationEmploys(g)
  RelationWorks_For(g)
  RelationDependent(g)
  RelationDependent_of(g)
  RelationControls(g)
  RelationControlled_by(g)
  RelationSupervisor(g)
  RelationSupervisee(g)
  Loadworks_on(g, sys.argv[1]+"/WORKS_ON.dat")
  addlocations(g, sys.argv[1]+"/DEPT_LOCATIONS.dat")


