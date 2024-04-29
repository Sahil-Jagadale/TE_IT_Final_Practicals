function buildTable(rows, cols, startValue, table) {
  var increment = startValue;
  for (var i = 0; i < rows; i++) {
      var row = table.insertRow(i);
      for (var j = 0; j < cols; j++) {
          var cell = row.insertCell(j);
          if (i === 0) {
              cell.textContent = increment++;
          } else {
              cell.textContent = parseInt(table.rows[0].cells[j].textContent) * (i+1);
          }
      }
  }
}

const form = document.querySelector('form')
form.addEventListener("submit", e=>{
  e.preventDefault();

  form.classList.add('was-validated')
  const inp1 = document.getElementById('inp1').value
  const inp2 = document.getElementById('inp2').value 
  var t1 = document.getElementById("t1")
  var t2 = document.getElementById("t2")
  var t3 = document.getElementById("t3");
  // splitting inputs
  const x = inp1.split("#").map(Number)
  const y = inp2.split("#").map(Number)
  // Now for table 1: x[0]-> row
  //                  x[1]-> cols
  //                  x[2]-> table starting value
  buildTable(x[0], x[1], x[2], t1)
  buildTable(y[0], y[1], y[2], t2)

  // Check if the number of rows and columns of the third table match those of the second table
  if (x[0] === y[0] && x[1] === y[1]) {

      // if first cell value of first table is same as 
      // first cell value of second table then print as it is
      // else print the multiplication of t1 and t2
      if(x[2] == y[2]){
          buildTable(y[0], y[1], y[2], t3);
      }
      else {
          // Perform multiplication of corresponding cells and populate t3
          for (var i = 0; i < x[0]; i++) {
              var row3 = t3.insertRow(i);
              for (var j = 0; j < x[1]; j++) {
                  var cell1 = t1.rows[i].cells[j];
                  var cell2 = t2.rows[i].cells[j];
                  var cell3 = row3.insertCell(j);
                  cell3.textContent = parseInt(cell1.textContent) * parseInt(cell2.textContent);
              }
          }
      }

  } else {
      document.getElementById("t3div").classList.add('hidden')
  }

})


// function buildTable(rows, cols, startValue, table){
//   var a=startValue;
//   for(var i=0;i<rows;i++){
//     var row=table.insertRow(i);
//     for(var j=0;j<cols;j++){
//       var cell=row.insertCell(j);
//       if(i===0){
//         cell.textContent = a++;
//       }
//       else{
//         cell.textContent = parseInt(table.rows[0].cells[j].textContent) * (i+1);
//       }
//     }
//   }
// }

// const form = document.querySelector("form");
// form.addEventListener("submit", e=> {
//   e.preventDefault();
//   form.classList.add("was-validated");
//   const inp1 = document.getElementById("inp1").value;
//   const inp2 = document.getElementById("inp2").value;
//   var t1=document.getElementById("t1");
//   var t2=document.getElementById("t2");
//   var t3=document.getElementById("t3");

//   var x=inp1.split("#").map(Number);
//   var y=inp2.split("#").map(Number);

//   buildTable(x[0], x[1], x[2], t1);
//   buildTable(y[0], y[1], y[2], t2);

//   if(x[0]==y[0] && x[1]==y[1]){
//     if(x[2]==y[2]){
//       buildTable(x[0],y[0],x[2],t3);
//     }
//     else{
//       for(var i=0;i<x[0];i++){
//         var row=t3.insertRow(i);
//         for(var j=0;j<x[1];j++){
//           var cell=row.insertCell(j);
//           var a=t1.rows[i].cells[j];
//           var b=t2.rows[i].cells[j];
//           cell.textContent = parseInt(a.textContent)*parseInt(b.textContent);
//         }
//       }
//     }
//   }
//   else{
//     document.getElementById("t3h").classList.add("hidden");
//   }
// })