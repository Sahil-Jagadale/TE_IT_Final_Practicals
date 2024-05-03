import { Component } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-signup',
  templateUrl: './signup.component.html',
  styleUrl: './signup.component.css'
})
export class SignupComponent {
  fullName: string = '';
  dob: Date = new Date();
  gender: string = '';
  contactNumber: string = '';
  email: string = '';
  hobbies: string = '';
  city: string = '';
  state: string = '';
  address: string = '';
  username: string = '';
  password: string = '';

  constructor(private router: Router) {}

  register() {
    // Validation
    if (!this.fullName || !this.dob || !this.gender || !this.contactNumber || !this.email ||
        !this.hobbies || !this.city || !this.state || !this.address || !this.username || !this.password) {
      alert('Please fill in all fields.');
      return;
    }

    // Simulate registration (store data locally or use any service)
    const userData = {
      fullName: this.fullName,
      dob: this.dob,
      gender: this.gender,
      contactNumber: this.contactNumber,
      email: this.email,
      hobbies: this.hobbies,
      city: this.city,
      state: this.state,
      address: this.address,
      username: this.username,
      password: this.password
    };
    localStorage.setItem('userData', JSON.stringify(userData));
    console.log('Registered Successfully');
    this.router.navigate(['/login']);
  }
}
