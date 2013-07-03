//console.log("Loaded content script with jQuery for " + window.location.href);
//document.addEventListener("keydown", keyDownTextField, false);//put here cant capture event

var seltext = null;
var keypressText = null;
var inTxt ="";

chrome.extension.onRequest.addListener(function(request, sender, sendResponse)
{
	switch(request.message)
	{
		case 'setText':
			window.seltext = request.data
			console.log("setText: "+seltext);
		break;
		
		case 'keydown1':
			window.keypressText = request.data
			rxKeyCode(keypressText);//style2
			console.log("bg keydown1: "+keypressText);	
		break;
		
		default:
			sendResponse({data: 'Invalid arguments'});
		break;
	}
});


//test keypress handling
//style2
function rxKeyCode(keypressText) {//accumulate the chars into string
console.log("rxKeyCode");
  var kcode = keypressText
  if(kcode==32) {
	console.log("You hit the SPACE key.");
	inTxt = inTxt + "+";//add separator for subsequent bi/tri/quad gram
	lookClue(inTxt);
  } else {
	console.log("You hit OTHER key.");
	inTxt = inTxt + keypressText;
	console.log("inTxt= "+inTxt);
  }
}//end rx key code

function lookClue(inTxt){
console.log("in look clue");
console.log("find on clue= "+inTxt);
//need to remove training space before query
var anyC = "ANY";
    chrome.tabs.create({ //open a new tab
	url: "http://www.clueue.com/search/?q="+ inTxt + anyC,//phrase search
    })


}//end lookClue
/*
//style1: doesnt work
document.addEventListener("keydown", keyDownTextField, false);
function keyDownTextField(event) {
console.log("inside keydown text field");
  var kcode = event.keyCode;
  if(kcode==32) {
    //alert("You hit the SPACE key.");
	console.log("You hit the enter key.");
	event.preventDefault();//If this method is called, the default action of the event will not be triggered.
  } else {
    //alert("Oh no you didn't.");
	console.log("You hit OTHER key.");
	event.preventDefault();
  }
}//end keyDownTextField
*/

//handling select text and right click context menu
//use with style1
function savetext(info,tab)
{
	var jax = new XMLHttpRequest();
	jax.open("POST","http://localhost/text/");
	//jax.open("POST","http://www.clueue.com/search/?q=asylum+ANY");//no disp in pop up window
	jax.setRequestHeader("Content-Type","application/x-www-form-urlencoded");
	jax.send("text="+seltext);
	jax.onreadystatechange = function() { if(jax.readyState==4) { alert(jax.responseText);	}}
}//end savetext

//use with style2
function getword(info,tab) {
    var any = "+ANY";
	
    console.log("Word \"" + info.selectionText + "\" was clicked.");//debug use
    chrome.tabs.create({ //open a new tab
        //url: "http://www.google.com/search?q=" + info.selectionText,//default
		//url: "http://www.clueue.com/search/?q="+ info.selectionText,//no phrase search
		url: "http://www.clueue.com/search/?q="+ info.selectionText + any,//phrase search
    })
}//end get word

//==create the right click context menu
/*
//style1 create context menu
var contexts = ["selection"];
for (var i = 0; i < contexts.length; i++)
{
    
	var context = contexts[i];
	chrome.contextMenus.create({
		"title": "Search localhost", 
		"contexts":[context], 
		"onclick": savetext
		});
    console.log("inside forloop context is "+context);	
}
*/

//style2 create context menu
chrome.contextMenus.create({
    title: "Clueue: %s", 
    contexts:["selection"], 
    onclick: getword,//call getWord()

});
