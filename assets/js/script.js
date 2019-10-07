var token = "";

var ws = new Philote({ url: "ws://localhost:6380" });
setTimeout(function(){
  ws.connect(token, function() {
    console.log("I'm here!");
  });
}, 1000);

ws.on("read-write-channel", function(data, event) {
  console.log(data);
});
ws.publish("read-write-channel", "data");
ws.disconnect();
