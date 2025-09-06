
import { Kafka  } from "kafkajs"

const kafka = new Kafka({ 
    clientId : "my-app", 
    brokers : ['localhost:9092']
})

let prices :any 
let currentPrice
let balance = 50000000 //in dollars no decimals
const openOrders :any =[]

const producer = kafka.producer(); 
async function connectPKafka() {
    await producer.connect(); 
}

connectPKafka()


const consumer = kafka.consumer({groupId:'group-2'})


async function connectCkafka() {
    await consumer.connect(); 
    await consumer.subscribe({ 
        topic: 'Q1' , fromBeginning : true
    })
    await consumer.run({ 
        eachMessage:async({topic , partition , message})=> { 
            const data = message?.value?.toString(); 
            if(data){
                try { 
                const maindata = JSON.parse(data)
                if(maindata.type == 'prices'){ 
                    prices = maindata.prices
                    console.log("price updated" + prices)
                    // console.log("reacched till here")
                }
                if(maindata.type == 'trade'){ 
                    console.log("reacched till here")

                    let data = maindata.trade
                    const assett :string = data.asset;
                    const reqId = data.reqId
                    const currentPrice =  prices?.[assett].price; 
                    const margin = data.margin 
                    const leverage = data.leverage;
                    const slippage = data.slippage
                    const type = data.type
                    if(balance > (margin*100)) { 
                        const orderId = Date.now()
                        balance = balance - (margin*100)
                        openOrders.push({ 
                            orderId : orderId , 
                            asset : assett , 
                            margin : margin , 
                            leverage : leverage , 
                            type : type , 
                            slippage : slippage  , 
                            openPrice : currentPrice , 
                            reqId : reqId
                        })
                        

                       await producer.send({ 
                            topic : "Q2",
                            messages : [{ 
                                value : JSON.stringify( { 
                                reqId : reqId ,
                                orderId : orderId , 
                                asset : assett , 
                                state : 'open',
                                margin : margin , 
                                leverage : leverage , 
                                type : type , 
                                slippage : slippage  , 
                                openPrice : currentPrice,
                            })
                            }]


                        })
                        console.log("reacched till here")
                        
                    }
                }
                if(maindata.type == 'closeOrder'){ 
                    console.log("aagya yha pr")
                    const data = maindata.trade; 
                    const orderId = data.orderId;
                    const reqId = data.reqId;
                    
                    console.log("order to close" + orderId)
                    //@ts-ignore
                    
                    const order = openOrders.find(o=>o.orderId === orderId)
                    console.log("found the order" + JSON.stringify(order))
                    const asset = order.asset
                    const currentPrice = prices?.[asset].price; 
                    const currentDecimal = prices?.[asset].decimal;
                    console.log("current price of the asset" + asset + "current price" + currentPrice + "current decimal " + currentDecimal)
                    const numberToEqualizeMargin = 10**(currentDecimal-2)
                    console.log('number to Equalize margin ' + numberToEqualizeMargin)
                    const quantity = ((order.margin * numberToEqualizeMargin)/order.openPrice); 
                    console.log("Quantitiy" + quantity)
                    const sellPrice = Math.trunc(currentPrice*quantity);
                    console.log("sell price  " + sellPrice)  
                
                    if(order.leverage == 1){ 
                        let profit 
                        if(order.type =='buy'){ 
                            profit = sellPrice - ((order.margin)*numberToEqualizeMargin);
                            
                        }
                        else if(order.type == 'sell'){ 
                            profit = ((order.margin)*numberToEqualizeMargin) - sellPrice;
                        }
                        console.log("order buy price  : " + order.margin*numberToEqualizeMargin)
                        //@ts-ignore
                        balance +=((profit/(10**(currentDecimal-4))) + ((order.margin)*100));
                        // balance +=  Math.trunc(sellPrice/10**(currentDecimal-4))

                        //@ts-ignore
                        console.log("to check , currentProfit = " + profit/(10**currentDecimal) + "balance : " + balance/(10**4))
                            await producer.send({ 
                            topic : "Q2",
                            messages : [{ 
                                value : JSON.stringify( { 
                                orderId : orderId , 
                                state : 'closed',
                                asset : asset , 
                                margin : order.margin , 
                                leverage : order.leverage , 
                                type : order.type , 
                                slippage : order.slippage  , 
                                openPrice : currentPrice , 
                                closedPrice : currentPrice  , 
                                reqId : reqId

                            })
                            }]


                        })
                    }
                    else if(order.leverage >1){ 
                        let profit 
                        if(order.type =='buy'){ 
                            profit = sellPrice - ((order.margin)*numberToEqualizeMargin);
                            
                        }
                        else if(order.type == 'sell'){ 
                            profit = ((order.margin)*numberToEqualizeMargin) - sellPrice;
                        }
                        console.log("order buy price  : " + order.margin*numberToEqualizeMargin)

                        if(profit){

                            profit*=order.leverage
                            balance +=((profit/(10**(currentDecimal-4))) + ((order.margin)*100));
                            //@ts-ignore
                            console.log("to check , currentProfit = " + profit/(10**currentDecimal) + "balance : " + balance/(10**4))
                            await producer.send({ 
                            topic : "Q2",
                            messages : [{ 
                                value : JSON.stringify( { 
                                orderId : orderId , 
                                state : 'closed',
                                asset : asset , 
                                margin : order.margin , 
                                leverage : order.leverage , 
                                type : order.type , 
                                slippage : order.slippage  , 
                                openPrice : currentPrice , 
                                closedPrice : currentPrice , 
                                reqId : reqId
                            })
                            }]


                        })
                        }

                    
                    }
                    
                }
                if(maindata.type == 'getBalance'){ 

                }
                if(maindata.type == 'SupportedAssets'){ 

                }
            } 
            catch(e) { 
                console.log("hehe" + e)
            }
            }
            else{ 
                // console.log(JSON.stringify(data))
            }
            
            
    }})
}
    

connectCkafka()





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
