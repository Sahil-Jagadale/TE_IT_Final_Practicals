import { Component } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrl: './login.component.css'
})
export class LoginComponent {
  username: string = '';
  password: string = '';

  constructor(private router: Router) {}

  login() {
    // Simulate login (retrieve data from local storage)
    const userData = JSON.parse(localStorage.getItem('userData') || '{}');
    if (userData.username === this.username && userData.password === this.password) {
      console.log('Logged in Successfully');
      this.router.navigate(['/profile']);
    } else {
      console.log('Invalid username or password');
    }
  }
}
