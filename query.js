$(document).ready(function() {
  var url = 'http://localhost:5000/company/departments/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var depts = response.departments;
      var htmlCode = "<select id='department' onchange=getDepartment()>"+
                     "<option id='None'>Select a Department</option>";
      for (var i=0; i<depts.length; i++) {
        htmlCode += "<option value='"+depts[i]+"'>"+depts[i]+"</option>";
      }
      htmlCode += "</select>";
      $("#department_select").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  });

   var url = 'http://localhost:5000/company/projects/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var projs = response.projects;
      var htmlCode = "<select id='project' onchange=getProject()>"+
                     "<option id='None'>Select a Project</option>";
      for (var i=0; i<projs.length; i++) {
        htmlCode += "<option value='"+projs[i]+"'>"+projs[i]+"</option>";
      }
      htmlCode += "</select>";
      $("#project_select").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  });

  var url = 'http://localhost:5000/company/employees/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var emps = response.employees;
      var htmlCode = "<select id='employee' onchange=getEmployee()>"+
                     "<option id='None'>Select an Employee</option>";
      for (var i=0; i<emps.length; i++) {
        htmlCode += "<option value='"+emps[i]+"'>"+emps[i]+"</option>";
      }
      htmlCode += "</select>";
      $("#employee_select").html(htmlCode);
      htmlCode = "<select id='emp_super' onchange=getSupervisees()>"+
                 "<option id='None'>Select an Employee</option>";
      for (var i=0; i<emps.length; i++) {
        htmlCode += "<option value='"+emps[i]+"'>"+emps[i]+"</option>";
      }
      htmlCode += "</select>";
      $("#employee_supervisee_select").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  });
  var url = 'http://localhost:5000/company/dcities/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var cities = response.cities;
      var htmlCode = "<select id='dcity' onchange=getDepartmentsByCity()>"+
                     "<option id='None'>Select a city</option>";
      for (var i=0; i<cities.length; i++) {
        htmlCode += "<option value='"+cities[i]+"'>"+cities[i]+"</option>";
      }
      htmlCode += "</select>";
      $("#department_by_city_select").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  });

  var url = 'http://localhost:5000/company/pcities/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var cities = response.cities;
      htmlCode = "<select id='pcity' onchange=getProjectsByCity()>"+
                 "<option id='None'>Select a City</option>";
      for (var i=0; i<cities.length; i++) {
        htmlCode += "<option value='"+cities[i]+"'>"+cities[i]+"</option>";
      }
      htmlCode += "</select>";
      $("#project_by_city_select").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  });
});
 function getSupervisees() {
  var ssn = $("#emp_super").val()
  var url = 'http://localhost:5000/company/supervisees/'+ssn+'/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var nElements = response.employees.length;
      console.log(nElements)
      var nRows = Math.floor(nElements/5);
      var remaining = nElements - 5*nRows;
      var htmlCode = "<div style=\"width: 100%; display: table;\">";
      for (var i=0; i<nRows; i++) {
        htmlCode += "<div style=\"display: table-row; height: 20px;\">";
        for (var j=0; j<5; j++) {
          htmlCode += "<div style=\"color:blue; width: 20%; display: table-cell;\"";
          htmlCode += " onClick='getEmployee2(\""+response.employees[5*i+j]+"\")'>";
          htmlCode += response.employees[5*i+j] + "</div>";
        }
        htmlCode += "</div>\n";
      }
      // process remaining
      htmlCode += "<div style=\"display: table-row; height: 20px;\">";
      for (var j=0; j<remaining; j++) {
        htmlCode += "<div style=\"color:blue; width: 20%; display: table-cell;\"";
        htmlCode += " onClick='getEmployee2(\""+response.employees[5*nRows+j]+"\")'>";
        htmlCode += response.employees[5*nRows+j] + "</div>";
      }
      htmlCode += "</div>\n";
      htmlCode += "</div>\n";
      //$("#element_div").html("");
      $("#results_div").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  });

}

function getDepartmentsByCity() {
  var cty = $("#dcity").val()
  var url = 'http://localhost:5000/company/departments/'+cty+'/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var nElements = response.departments.length;
      var nRows = Math.floor(nElements/5);
      var remaining = nElements - 5*nRows;
      var htmlCode = "<div style=\"width: 100%; display: table;\">";
      for (var i=0; i<nRows; i++) {
        htmlCode += "<div style=\"display: table-row; height: 20px;\">";
        for (var j=0; j<5; j++) {
          htmlCode += "<div style=\"color:blue; width: 20%; display: table-cell;\"";
          htmlCode += " onClick='getDepartment2(\""+response.departments[5*i+j].dnumber+"\")'>";
          htmlCode += response.departments[5*i+j].dname + "</div>";
        }
        htmlCode += "</div>\n";
      }
      // process remaining
      htmlCode += "<div style=\"display: table-row; height: 20px;\">";
      for (var j=0; j<remaining; j++) {
        htmlCode += "<div style=\"color:blue; width: 20%; display: table-cell;\"";
        htmlCode += " onClick='getDepartment2(\""+response.departments[5*nRows+j].dnumber+"\")'>";
        htmlCode += response.departments[5*nRows+j].dname + "</div>";
      }
      htmlCode += "</div>\n";
      htmlCode += "</div>\n";
      //$("#element_div").html("");
      $("#results_div").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  });

}

function getProjectsByCity() {
  var cty = $("#pcity").val()
  var url = 'http://localhost:5000/company/projects/'+cty+'/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var nElements = response.projects.length;
      var nRows = Math.floor(nElements/5);
      var remaining = nElements - 5*nRows;
      var htmlCode = "<div style=\"width: 100%; display: table;\">";
      for (var i=0; i<nRows; i++) {
        htmlCode += "<div style=\"display: table-row; height: 20px;\">";
        for (var j=0; j<5; j++) {
          htmlCode += "<div style=\"color:blue; width: 20%; display: table-cell;\"";
          htmlCode += " onClick='getProject2(\""+response.projects[5*i+j].pnumber+"\")'>";
          htmlCode += response.projects[5*i+j].pname + "</div>";
        }
        htmlCode += "</div>\n";
      }
      // process remaining
      htmlCode += "<div style=\"display: table-row; height: 20px;\">";
      for (var j=0; j<remaining; j++) {
        htmlCode += "<div style=\"color:blue; width: 20%; display: table-cell;\"";
        htmlCode += " onClick='getProject2(\""+response.projects[5*nRows+j].pnumber+"\")'>";
        htmlCode += response.projects[5*nRows+j].pname + "</div>";
      }
      htmlCode += "</div>\n";
      htmlCode += "</div>\n";
      //$("#element_div").html("");
      $("#results_div").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  })

}

function getDepartment() {
  var dno = $("#department").val()
  var url = 'http://localhost:5000/company/department/'+dno+'/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var htmlCode = "<table>";
      htmlCode += "<tr valign=\"top\"><td><table>";
      htmlCode += "<tr><th align=\"left\">DNAME:</th><td>"+response.dname+"</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">MANAGER:</th><td>" +
                  "<div style=\"color:blue;\" onClick='getEmployee2(\""+response.mgrssn+"\")'>" +
                  response.mgrssn + "</div>";
      htmlCode += "<tr><th align=\"left\">MGRSTARTDATE:</th>" +
                  "<td>"+response.manager_start_date+"</td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">LOCATIONS</th></tr>\n";
      for (var i=0; i<response.locations.length; i++)
        htmlCode += "<tr><td>"+response.locations[i] + "</td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">EMPLOYEES:</th></tr>\n";
      for (var i=0; i<response.employees.length; i++)
        htmlCode += "<tr><td><div style=\"color:blue;\" onClick='getEmployee2(\""+
                  response.employees[i] +"\")'>" +
                  response.employees[i] + "</div></td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">PROJECTS:</th></tr>\n";
      for (var i=0; i<response.controlled_projects.length; i++)
        htmlCode += "<tr><td><div style=\"color:blue;\" onClick='getProject2("+
                    response.controlled_projects[i].pnumber +")'>" +
                    response.controlled_projects[i].pname + "</div></tr></td>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "</tr></table>\n";
      $("#department_div").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  })
}

function getDepartment2(dno) {
  var url = 'http://localhost:5000/company/department/'+dno+'/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var htmlCode = "<table>";
      htmlCode += "<tr valign=\"top\"><td><table>";
      htmlCode += "<tr><th align=\"left\">DNAME:</th><td>"+response.dname+"</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">MANAGER:</th><td>" +
                  "<div style=\"color:blue;\" onClick='getEmployee2(\""+response.mgrssn+"\")'>" +
                  response.mgrssn + "</div>";
      htmlCode += "<tr><th align=\"left\">MGRSTARTDATE:</th>" +
                  "<td>"+response.manager_start_date+"</td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">LOCATIONS</th></tr>\n";
      for (var i=0; i<response.locations.length; i++)
        htmlCode += "<tr><td>"+response.locations[i] + "</td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">EMPLOYEES:</th></tr>\n";
      for (var i=0; i<response.employees.length; i++)
        htmlCode += "<tr><td><div style=\"color:blue;\" onClick='getEmployee2(\""+
                  response.employees[i] +"\")'>" +
                  response.employees[i] + "</div></td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">PROJECTS:</th></tr>\n";
      for (var i=0; i<response.controlled_projects.length; i++)
        htmlCode += "<tr><td><div style=\"color:blue;\" onClick='getProject2("+
                    response.controlled_projects[i].pnumber +")'>" +
                    response.controlled_projects[i].pname + "</div></tr></td>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "</tr></table>\n";
      $("#department_div").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  })
}

function getProject() {
  var pno = $("#project").val()
  var url = 'http://localhost:5000/company/project/'+pno+'/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var htmlCode = "<table>";
      htmlCode += "<tr valign=\"top\"><td><table>";
      htmlCode += "<tr><th align=\"left\">PNAME:</th><td>"+response.pname+"</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">LOCATION:</th><td>";
      htmlCode += response.plocation;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">DEPARTMENT:</th><td>" +
                  "<div style=\"color:blue;\" onClick='getDepartment2("+
                  response.controlling_dnumber+")'>" +
                  response.controlling_dname + "</div>";
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">PERSON HOURS:</th><td>"+
                  response.person_hours+"</td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";

      htmlCode += "<tr><th align=\"left\">EMPLOYEES:</th></tr>\n";
      for (var i=0; i<response.employees.length; i++)
        htmlCode += "<tr><td><div style=\"color:blue;\" onClick='getEmployee2(\""+
                    response.employees[i] + "\")'>" +
                    response.employees[i] + "</div></td></tr>\n";
      htmlCode += "</table></td>\n";

      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">DEPT HOURS:</th></tr>\n";
      for (var key in response.dept_hours)
        htmlCode += "<tr><td>"+ key +
                    " ("+ response.dept_hours[key] + ")</td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "</tr></table>\n";
      //console.log(htmlCode);
      $("#project_div").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  })
}

function getProject2(pno) {
  var url = 'http://localhost:5000/company/project/'+pno+'/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var htmlCode = "<table>";
      htmlCode += "<tr valign=\"top\"><td><table>";
      htmlCode += "<tr><th align=\"left\">PNAME:</th><td>"+response.pname+"</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">LOCATION:</th><td>";
      htmlCode += response.plocation;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">DEPARTMENT:</th><td>" +
                  "<div style=\"color:blue;\" onClick='getDepartment2("+
                  response.controlling_dnumber+")'>" +
                  response.controlling_dname + "</div>";
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">PERSON HOURS:</th><td>"+
                  response.person_hours+"</td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";

      htmlCode += "<tr><th align=\"left\">EMPLOYEES:</th></tr>\n";
      for (var i=0; i<response.employees.length; i++)
        htmlCode += "<tr><td><div style=\"color:blue;\" onClick='getEmployee2(\""+
                    response.employees[i] + "\")'>" +
                    response.employees[i] + "</div></td></tr>\n";
      htmlCode += "</table></td>\n";

      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">DEPT HOURS:</th></tr>\n";
      for (var key in response.dept_hours)
        htmlCode += "<tr><td>"+ key +
                    " ("+ response.dept_hours[key] + ")</td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "</tr></table>\n";
      //console.log(htmlCode);
      $("#project_div").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  })
}

function getEmployee() {
  var ssn = $("#employee").val()
  var url = 'http://localhost:5000/company/employee/'+ssn+'/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var htmlCode = "<table>";
      htmlCode += "<tr valign=\"top\"><td><table>";
      htmlCode += "<tr><th align=\"left\">NAME:</th><td>"+
                  response.fname+" "+response.minit+" "+response.lname+"</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">GENDER:</th><td>";
      htmlCode += response.gender;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">ADDRESS:</th><td>";
      htmlCode += response.address;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">BIRTHDATE:</th><td>";
      htmlCode += response.bdate;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">SALARY:</th><td>";
      htmlCode += response.salary;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">WORKS FOR:</th><td>" +
                  "<div style=\"color:blue;\" onClick='getDepartment2("+
                  response.department_number+")'>" +
                  response.department_name + "</div>";
      htmlCode += "</td></tr>\n";
      if (response.hasOwnProperty("manages")) {
        htmlCode += "<tr><th align=\"left\">MANAGES:</th><td>" +
                    "<div style=\"color:blue;\" onClick='getDepartment2("+
                    response.manages.dnumber+")'>" +
                    response.manages.dname + "</div>";
        htmlCode += "</td></tr>\n";
      }
      if (response.hasOwnProperty("supervisor")) {
        htmlCode += "<tr><th align=\"left\">SUPERVISOR:</th><td>" +
            "<div style=\"color:blue;\" onClick='getEmployee2(\""+
            response.supervisor+"\")'>" +
            response.supervisor + "</div>";
        htmlCode += "</td></tr>\n";
      }
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">PROJECTS:</th></tr>\n";
      for (var i=0; i<response.projects.length; i++)
        htmlCode += "<tr><td><div style=\"color:blue;\" onClick='getProject2(\""+
                    response.projects[i].pnumber + "\")'>" +
                    response.projects[i].pname + " ("+
                    response.projects[i].hours + ")</div></td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">DEPENDENTS:</th></tr>\n";
      for (var i=0; i<response.dependents.length; i++)
        htmlCode += "<tr><td>" + response.dependents[i].dname +
                    " ("+response.dependents[i].relationship+":"+
                    response.dependents[i].gender+":"+
                    response.dependents[i].bdate+")</td></tr>\n";
      htmlCode += "</table></td>\n";

      htmlCode += "</tr></table>\n";
      //console.log(htmlCode);
      $("#employee_div").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  })
}

function getEmployee2(ssn) {
  var url = 'http://localhost:5000/company/employee/'+ssn+'/';
  $.ajax({
    url: url,
    type: 'GET',
    success: function(response) {
      var htmlCode = "<table>";
      htmlCode += "<tr valign=\"top\"><td><table>";
      htmlCode += "<tr><th align=\"left\">NAME:</th><td>"+
                  response.fname+" "+response.minit+" "+response.lname+"</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">GENDER:</th><td>";
      htmlCode += response.gender;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">ADDRESS:</th><td>";
      htmlCode += response.address;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">BIRTHDATE:</th><td>";
      htmlCode += response.bdate;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">SALARY:</th><td>";
      htmlCode += response.salary;
      htmlCode += "</td></tr>\n";
      htmlCode += "<tr><th align=\"left\">WORKS FOR:</th><td>" +
                  "<div style=\"color:blue;\" onClick='getDepartment2("+
                  response.department_number+")'>" +
                  response.department_name + "</div>";
      htmlCode += "</td></tr>\n";
      if (response.hasOwnProperty("manages")) {
        htmlCode += "<tr><th align=\"left\">MANAGES:</th><td>" +
                    "<div style=\"color:blue;\" onClick='getDepartment2("+
                    response.manages.dnumber+")'>" +
                    response.manages.dname + "</div>";
        htmlCode += "</td></tr>\n";
      }
      if (response.hasOwnProperty("supervisor")) {
        htmlCode += "<tr><th align=\"left\">SUPERVISOR:</th><td>" +
            "<div style=\"color:blue;\" onClick='getEmployee2(\""+
            response.supervisor+"\")'>" +
            response.supervisor + "</div>";
        htmlCode += "</td></tr>\n";
      }
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">PROJECTS:</th></tr>\n";
      for (var i=0; i<response.projects.length; i++)
        htmlCode += "<tr><td><div style=\"color:blue;\" onClick='getProject2(\""+
                    response.projects[i].pnumber + "\")'>" +
                    response.projects[i].pname + " ("+
                    response.projects[i].hours + ")</div></td></tr>\n";
      htmlCode += "</table></td>\n";
      htmlCode += "<td><table>\n";
      htmlCode += "<tr><th align=\"left\">DEPENDENTS:</th></tr>\n";
      for (var i=0; i<response.dependents.length; i++)
        htmlCode += "<tr><td>" + response.dependents[i].dname +
                    " ("+response.dependents[i].relationship+":"+
                    response.dependents[i].gender+":"+
                    response.dependents[i].bdate+")</td></tr>\n";
      htmlCode += "</table></td>\n";

      htmlCode += "</tr></table>\n";
      //console.log(htmlCode);
      $("#employee_div").html(htmlCode);
    },
    error: function(error) {
      alert("ERROR");
      console.log(error);
    }
  })
}
