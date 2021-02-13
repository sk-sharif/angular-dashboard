import { Component, OnInit } from '@angular/core';
import { OrderService } from './../../service/order/order.service';

@Component({
  selector: 'app-order',
  templateUrl: './order.component.html',
  styleUrls: ['./order.component.scss']
})
export class OrderComponent implements OnInit {
  orderList:any;

  constructor(private orderService:OrderService) { }

  ngOnInit(): void {
    this.initializeOrder();
  }
  initializeOrder(){
    this.orderService.getOrders(0,100).subscribe(response=>{
      if(response){
        this.orderList = response.content;
        console.log("orders:",response.content);
      }
    })
  }

}
