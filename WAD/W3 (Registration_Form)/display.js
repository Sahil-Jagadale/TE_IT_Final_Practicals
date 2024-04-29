var users = JSON.parse(localStorage.getItem('users')) || [];

var tbody = document.getElementById('usertb');

users.forEach((user) => {
  var row = tbody.insertRow();
  row.insertCell(0).textContent = user.name;
  row.insertCell(1).textContent = user.email;
  row.insertCell(2).textContent = user.password;
  row.insertCell(3).textContent = user.address;
  row.insertCell(4).textContent = user.city;
  row.insertCell(5).textContent = user.district;
  row.insertCell(6).textContent = user.state;
  row.insertCell(7).textContent = user.country;
  row.insertCell(8).textContent = user.pin;
});
