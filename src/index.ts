
import { Kafka  } from "kafkajs"

const kafka = new Kafka({ 
    clientId : "my-app", 
    brokers : ['localhost:9092']
})
const producer = kafka.producer(); 
async function connectPKafka() {
    await producer.connect(); 
    await producer.send({ 
        topic : "Q2",
        messages : [{ 
            value : "ahhhhhh ahhhhhhh"
        }]
    })
}

connectPKafka()

const consumer = kafka.consumer({groupId:'group-1'})
async function connectCkafka() {
    await consumer.connect(); 
    await consumer.subscribe({ 
        topic: 'Q1' , fromBeginning : true
    })
    await consumer.run({ 
        eachMessage:async({topic , partition , message})=> { 
            console.log({
                offset : message.offset, 
                value : message?.value?.toString()
            })
        }
    })
}
connectCkafka()
const prices :any = []
let currentPrice
let balance = 500000 //in dollars no decimals
const openOrders :any =[]




//we can either create multiple publishers and subscribers or just create one publisher in which we can find through message that what user want to do
//one pubsub which will take message from the queue 
//redis streams will also work 







// client.subscribe('order' , (message)=> { 
//     //order - to buy a order or shot or long a order 
//     const data = JSON.parse(message); 
//     console.log(data)
    
//     const asset = data.asset; 
//     //@ts-ignore
//     currentPrice= prices.findLast(o => o.asset === asset)
//     const type = data.type;
//     const margin = data.margin/100;  //2 decimals
//     const leverage = data.leverage;
//     const slippage = data.slippage/100 //2decimals
    
//     if(balance > currentPrice) { 
//         const orderId = Date.now()

//         balance = balance - margin
//         openOrders.push({ 
//             orderId : orderId , 
//             asset : asset , 
//             margin : margin , 
//             leverage : leverage , 
//             type : type , 
//             slippage : slippage  , 
//             openPrice : currentPrice
//         })
//         //send the message that order is completed with order id , mostly well do
//         pub(`orderID : ${orderId}`)
//     }
    
//     else { 
//         pub(`not enough balance `)

//     }
    
    
// })




// client.subscribe('close' , (message)=> { 
//     const data = JSON.parse(message)
//     console.log(data)

//     const orderID = data.orderId;
//     if(orderID){ 
//         //@ts-ignore
//        const order =  openOrders.find(o=>o.orderId === orderID)
//        const asset = order.asset
//        //@ts-ignore
//        const currentPrice = prices.findLast(o=> o.asset== asset)
//        const quantity = order.openPrice / order.margin
//        if(order.type == 'long'){ 
//         let profit = (currentPrice*quantity)-order.margin
//         balance += profit;
//         //mark the order as close or pop it out from the open orders array 
//        }
//        else if(order.type == 'shot'){ 
//         let profit = order.margin - (currentPrice*quantity)
//         balance += profit;
//        }
//     }
// })

// function pub(message : any){ 
//     client.publish('messages' , JSON.stringify(message))
// }



// setInterval(()=> { 
    
// } , 1000)
