var form = document.getElementById("form").addEventListener("submit", (e) => {
  e.preventDefault();
  var name = document.getElementById("fname").value;
  var email = document.getElementById("femail").value;
  var password = document.getElementById("fpass").value;
  var cpassword = document.getElementById("fcpass").value;
  var address = document.getElementById("fadd").value;
  var city = document.getElementById("fcity").value;
  var district = document.getElementById("fdis").value;
  var state = document.getElementById("fstate").value;
  var country = document.getElementById("fcnt").value;
  var pin = document.getElementById("fpin").value;

  if (password.length < 4) {
    alert("Password must be at least 4 characters long");
    return;
  }

  if (password !== cpassword) {
    alert("Passwords do not match");
    return;
  }

  var user = {
    name: name,
    email: email,
    password: password,
    cpassword: cpassword,
    address: address,
    city: city,
    district: district,
    state: state,
    country: country,
    pin: pin
  };

  var users = JSON.parse(localStorage.getItem('users')) || [];
  users.push(user);
  localStorage.setItem('users', JSON.stringify(users));

  window.location.href = 'display.html';
});