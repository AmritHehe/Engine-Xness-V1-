
import { Kafka  } from "kafkajs"
import { PrismaClient } from "../src/generated/prisma/index.js"
import { emitWarning } from "process"
const kafka = new Kafka({ 
    clientId : "my-app", 
    brokers : ['localhost:9092']
})

let prices :any 
let currentPrice
let balance = 50000000 //in dollars , 4 decimals
let offset : number

const prisma = new PrismaClient()

let users = [{ 
    email : 'bhosdu@gmail.com' , 
    balance : 5000 , 
    openOrders : [{ 
        orderId : 394394589 , 
        asset : 'ETH' , 
        margin : 100000 , 
        leverage : 1 , 
        type : 'sell' , 
        slippage : 0  , 
        openPrice : 1302434231231 
    }]
}]

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
            offset = Number(message.offset);
            const data = message?.value?.toString(); 
            if(data){
                try { 
                const maindata = JSON.parse(data)
                if(maindata.type == 'user'){ 
                    console.log('reached here ? ')
                    let user = maindata.user
                    const reqId = user.reqId
                    //@ts-ignore
                    const find = users.find(u => u.email === user.email)
                    console.log("user : " + user , " reqId" + reqId) 
                    console.log("find" + find)
                    if(!find){ 
                        console.log('inside here')
                        const pushUser = { 
                            email : user.email , 
                            balance : 50000000 , 
                            openOrders : []
                        }
                        users.push(pushUser)
                        await producer.send({ 
                            topic : "Q2",
                            messages : [{ 
                                value : JSON.stringify( { 
                                    type : 'user' ,
                                    reqId : reqId , 
                                    message : 'done'
                                })
                            }]
                        })
                        console.log('req.id ' + reqId)
                        
                    }
                    else { 
                        console.log('offo')
                        await producer.send({ 
                            topic : "Q2",
                            messages : [{ 
                                value : JSON.stringify( { 
                                    reqId : reqId ,
                                    message : "user already exits"
                                })
                            }]


                        })
                    }

                }
                if(maindata.type == 'prices'){ 
                    prices = maindata.prices
                    console.log("price updated" + prices)
                    // console.log("reacched till here")
                }
                if(maindata.type == 'trade'){ 
                    console.log("reacched till here")
                    
                    let data = maindata.trade ; 
                    let email = data.email ; 
                    const reqId = data.reqId

                    let user = users.find(u=> u.email === email )
                    if(user){
                        const assett :string = data.asset;
                        const currentPrice =  prices?.[assett].price; 
                        const margin = data.margin 
                        const leverage = data.leverage;
                        const slippage = data.slippage
                        const type = data.type
                        if(user.balance > (margin*100)) { 
                            const orderId = Date.now()
                            user.balance = user.balance - (margin*100)
                            user.openOrders.push({ 
                                orderId : orderId , 
                                asset : assett , 
                                margin : margin , 
                                leverage : leverage , 
                                type : type , 
                                slippage : slippage  , 
                                openPrice : currentPrice , 
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
                                email : email
                             })
                            }]


                        })
                        console.log("reacched till here")
                    }
                    
                        
                    }
                    else { 
                       await producer.send({ 
                            topic : "Q2",
                            messages : [{ 
                                value : JSON.stringify( { 
                                reqId : reqId ,
                                message : 'failed'
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
                    const email = data.email;
                    let user = users.find(u=> u.email === email )
                    if(user){ 
                        console.log("order to close" + orderId)
                        //@ts-ignore
                        
                        const order = user.openOrders.find(o=>o.orderId === orderId)
                        console.log("found the order" + JSON.stringify(order))
                        if(order){
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
                                console.log('balance before' + balance)
                                console.log("order buy price  : " + order.margin*numberToEqualizeMargin)
                                //@ts-ignore
                                user.balance +=((profit/(10**(currentDecimal-4))) + ((order.margin)*100));
                                // balance +=  Math.trunc(sellPrice/10**(currentDecimal-4))

                                //@ts-ignore
                                console.log("to check , currentProfit = " + profit/(10**currentDecimal) + "balance : " + user.balance/(10**4))
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
                                        reqId : reqId ,
                                        email : email , 
                                        profit : profit
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
                                    user.balance +=((profit/(10**(currentDecimal-4))) + ((order.margin)*100));
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
                                        reqId : reqId , 
                                        email : email
                                    })
                                    }]


                                })
                                }

                                    
                            }
                        }
                        
                    }
                    else { 
                        await producer.send({ 
                            topic : "Q2",
                            messages : [{ 
                                value : JSON.stringify( { 
                                reqId : reqId ,
                                message : 'failed because he cant fin the user'
                            })
                            }]


                        })
                    }
                    
                    
                }
                if(maindata.type == 'getBalance'){ 
                    const reqId = maindata.data.reqId; 
                    const email = maindata.data.email
                    const user = users.find(u => u.email === email)
                    const balance = user?.balance
                    await producer.send({ 
                        topic  : 'Q2', 
                        messages : [{ 
                            value  : JSON.stringify({ 
                                state : "balance" ,
                                balance : balance , 
                                id : reqId
                            })
                        }]
                    })
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


setInterval(async ()=> { 
    try{ 
        await prisma.snap.create({
            data : { 
                openOrders : JSON.stringify(openOrders) , 
                balance : balance , 
                offsetId : offset
            }
        }) ; 
        console.log("snapshotted the db " )
    }
    catch(err){ 
        console.error('hahaha fail hogis')
    }
    
} , 60000)


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
