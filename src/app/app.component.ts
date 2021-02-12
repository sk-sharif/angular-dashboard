import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
    toggle = true;

    handleToggle(){
      console.log("toggle Status", this.toggle);
      this.toggle = !this.toggle;
    }
}
