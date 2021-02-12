import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
    toggle = false;

    handleToggle(){
      console.log("toggle Status", this.toggle);
      this.toggle = !this.toggle;
    }
}
