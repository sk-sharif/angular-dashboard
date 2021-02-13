import { Component, Input, OnInit } from '@angular/core';

@Component({
  selector: 'app-sidemenu',
  templateUrl: './sidemenu.component.html',
  styleUrls: ['./sidemenu.component.scss']
})
export class SidemenuComponent implements OnInit {

  @Input() toggle: any;

  constructor() { }

  ngOnInit(): void {
  }
 hidden=false;
  
    handleHidden(){
      console.log("toggle Status", this.hidden);
      this.hidden = !this.hidden;
    }


}
