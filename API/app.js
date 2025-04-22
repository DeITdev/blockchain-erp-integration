const express = require('express');
const app = express();
const bodyparser = require('body-parser');
const { deploy, store, read } = require('./contract');
app.use(bodyparser.json());

app.post("/deploy", async (req, res) => {
  let privateKey = req.body.privateKey;
  let result = await deploy(privateKey);
  res.send(result);
})


app.post("/store", async (req, res) => {
  let privateKey = req.body.privateKey;
  let contractAddress = req.body.contractAddress;
  let value = req.body.value;
  let result = await store(privateKey, contractAddress, value)
  res.send(result)

})

app.get("/read", async (req, res) => {
  let contractAddress = req.query.contractAddress;
  let result = await read(contractAddress);
  res.send(result)
})

app.post("/deploySimple", async (req, res) => {
  let privateKey = req.body.privateKey;
  let result = await deploySimple(privateKey);
  res.send(result);
})

app.listen(4001, () => {
  console.log("server started")
})