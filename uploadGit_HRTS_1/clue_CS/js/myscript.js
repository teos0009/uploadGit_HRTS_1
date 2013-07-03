//this is a "content script" that is sandbox/rendered by chrome. chk chrome faq
//this js is injected according to param set at manifest.json

document.addEventListener('mouseup',function(event)
{
	var sel = window.getSelection().toString();
	
	if(sel.length)
		chrome.extension.sendRequest({
		'message':'setText',
		'data': sel
		},function(response){})
})

/*
//style 1: doesnt work
document.addEventListener("keydown", keyDownTextField, false);

function keyDownTextField(e) {
console.log("myscr inside keydown text field");
  var kcode = e.keyCode;
  if(kcode==32) {
    //alert("You hit the SPACE key.");
	console.log("You hit the enter key.");
	//event.preventDefault();//If this method is called, the default action of the event will not be triggered.
  } else {
    //alert("Oh no you didn't.");
	console.log("You hit OTHER key.");
    //event.preventDefault();
  }
}//end keyDownTextField
*/

//document.addEventListener('keydown', function(event) {//ok
//console.log("keycode: " + event.keyCode);
//})


/*
//style2 works
//console out= bg keydown1: my scr keydown: 32
document.addEventListener('keydown',function(event)
{
    //var sel = event.keyCode;
	
	var sel = String.fromCharCode(event.keyCode);
	
	//console no display keycode
	//var kcode = e.keyCode;
	//if(kcode==32) {
	//	console.log("You hit the SPACE key.");
	//	alert("Oh no you SPACE");
	//} else {
	//console.log("You hit OTHER key.");
	//alert("Oh no you OTHER");
	//}	
	
	//var sel = "my scr keydown: " + event.keyCode;//debug only
	//send to bg.js to process
	chrome.extension.sendRequest({
	'message':'keydown1',
	'data': sel
	},function(response){})

});
*/
//style3: use keypress instead of keydown to get case sensitive ascii
//works with seacrch box, but not on googld docs. GWDC has a way of dealing with it
document.addEventListener('keypress',function(event)
{    //var sel = event.keyCode;
	//var sel = String.fromCharCode(event.keyCode);
	var sel = event.keyCode;
	if(sel==32){//need space bar, and also other punct marks to decide word boundary
	sel = 32;
	}
   else{//punct marks handled in ascii with fromCharCode
   sel = String.fromCharCode(event.keyCode);
   }
	//var sel = "my scr keydown: " + event.keyCode;//debug only
	//send to bg.js to process
	chrome.extension.sendRequest({
	'message':'keydown1',
	'data': sel
	},function(response){})

});