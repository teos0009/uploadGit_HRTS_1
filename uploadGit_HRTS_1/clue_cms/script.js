function getword(info,tab) {
    var any = "+ANY";
    console.log("Word " + info.selectionText + " was clicked.");
    chrome.tabs.create({ 

	    
        //url: "http://www.google.com/search?q=" + info.selectionText,
		//url: "http://www.clueue.com/search/?q="+ info.selectionText,
		url: "http://www.clueue.com/search/?q="+ info.selectionText + any,
    })


}

chrome.contextMenus.create({

    title: "Clueue: %s", 
    contexts:["selection"], 
    onclick: getword,

});