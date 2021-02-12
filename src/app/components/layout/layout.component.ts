import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-layout',
  templateUrl: './layout.component.html',
  styleUrls: ['./layout.component.scss']
})
export class LayoutComponent implements OnInit {

  constructor() { }

  ngOnInit(): void {
  }
  toggle = true;
  
    handleToggle(){
      console.log("toggle Status", this.toggle);
      this.toggle = !this.toggle;
    }

}
