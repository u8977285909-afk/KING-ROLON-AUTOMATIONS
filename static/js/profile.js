/* =========================================================
KING ROLON AUTOMATIONS — profile.js
Perfil: follow toggle + banner editor
========================================================= */

(function(){

"use strict";

function safeJson(response){
return response.json().catch(function(){ return {}; });
}

function initFollowButton(){

const btn=document.querySelector(".kr-follow-btn-premium");
if(!btn) return;

const url=btn.getAttribute("data-follow-url");
const followersEl=document.getElementById("kr-stat-followers");
const textEl=btn.querySelector(".kr-follow-btn-text");

btn.addEventListener("click",async function(){

if(!url || btn.disabled) return;

const wasFollowing=btn.getAttribute("data-following")==="1";
const oldText=textEl?textEl.textContent:btn.textContent;

btn.disabled=true;

if(textEl){textEl.textContent="…";}
else{btn.textContent="…";}

try{

const res=await fetch(url,{
method:"POST",
headers:{Accept:"application/json"},
credentials:"same-origin"
});

const data=await safeJson(res);

if(res.ok && data.ok){

const isFollowing=!!data.following;

btn.setAttribute("data-following",isFollowing?"1":"0");
btn.classList.toggle("is-following",isFollowing);

if(textEl){
textEl.textContent=isFollowing?"Siguiendo":"Seguir";
}else{
btn.textContent=isFollowing?"Siguiendo":"Seguir";
}

if(followersEl && wasFollowing!==isFollowing){

const current=parseInt(followersEl.textContent||"0",10)||0;

followersEl.textContent=String(
isFollowing?current+1:Math.max(0,current-1)
);

}

}else{

if(textEl){textEl.textContent=oldText;}
else{btn.textContent=oldText;}

}

}catch(e){

if(textEl){textEl.textContent=oldText;}
else{btn.textContent=oldText;}

}finally{

btn.disabled=false;

}

});

}

function initBannerEditor(){

const banner=document.querySelector('[data-banner-editor="1"]');
const img=document.getElementById("krProfileBannerImage");

const moveBtn=document.getElementById("krBannerMoveBtn");
const zoomInBtn=document.getElementById("krBannerZoomInBtn");
const zoomOutBtn=document.getElementById("krBannerZoomOutBtn");
const resetBtn=document.getElementById("krBannerResetBtn");
const saveBtn=document.getElementById("krBannerSaveBtn");

if(!banner || !img || !saveBtn) return;

let x=parseFloat(banner.dataset.x||"50")||50;
let y=parseFloat(banner.dataset.y||"50")||50;
let scale=parseFloat(banner.dataset.scale||"1")||1;

let moveMode=false;
let dragging=false;
let startX=0;
let startY=0;
let originX=x;
let originY=y;

function clamp(v,min,max){
return Math.max(min,Math.min(max,v));
}

function render(){

img.style.transform=
"translate(-"+(50-x)+"%, -"+(50-y)+"%) scale("+scale+")";

banner.classList.toggle("is-move-mode",moveMode);

if(moveBtn){
moveBtn.classList.toggle("is-active",moveMode);
}

}

function setDirty(v){
saveBtn.classList.toggle("is-dirty",!!v);
}

function beginDrag(cx,cy){

if(!moveMode) return;

dragging=true;
startX=cx;
startY=cy;
originX=x;
originY=y;

banner.classList.add("is-dragging");

}

function doDrag(cx,cy){

if(!dragging) return;

const rect=banner.getBoundingClientRect();
if(!rect.width || !rect.height) return;

const dx=((cx-startX)/rect.width)*100;
const dy=((cy-startY)/rect.height)*100;

x=clamp(originX+dx,-100,200);
y=clamp(originY+dy,-100,200);

render();
setDirty(true);

}

function endDrag(){

dragging=false;
banner.classList.remove("is-dragging");

}

banner.addEventListener("mousedown",function(e){
if(!moveMode) return;
e.preventDefault();
beginDrag(e.clientX,e.clientY);
});

window.addEventListener("mousemove",function(e){
doDrag(e.clientX,e.clientY);
});

window.addEventListener("mouseup",endDrag);

if(moveBtn){
moveBtn.addEventListener("click",function(){
moveMode=!moveMode;
render();
});
}

if(zoomInBtn){
zoomInBtn.addEventListener("click",function(){
scale=clamp(scale+0.08,0.6,3);
render();
setDirty(true);
});
}

if(zoomOutBtn){
zoomOutBtn.addEventListener("click",function(){
scale=clamp(scale-0.08,0.6,3);
render();
setDirty(true);
});
}

if(resetBtn){
resetBtn.addEventListener("click",function(){
x=50;
y=50;
scale=1;
render();
setDirty(true);
});
}

saveBtn.addEventListener("click",async function(){

if(saveBtn.disabled) return;

saveBtn.disabled=true;

const oldText=saveBtn.textContent;
saveBtn.textContent="Guardando...";

try{

const res=await fetch("/api/profile/banner/transform",{

method:"POST",
headers:{
"Content-Type":"application/json",
Accept:"application/json"
},
credentials:"same-origin",
body:JSON.stringify({x,y,scale})

});

const data=await safeJson(res);

if(res.ok && data.ok){

banner.dataset.x=String(x);
banner.dataset.y=String(y);
banner.dataset.scale=String(scale);

saveBtn.textContent="Guardado";
saveBtn.classList.remove("is-dirty");

setTimeout(()=>{saveBtn.textContent="Guardar";},900);

}else{

saveBtn.textContent="Error";
setTimeout(()=>{saveBtn.textContent=oldText;},1200);

}

}catch(e){

saveBtn.textContent="Error";
setTimeout(()=>{saveBtn.textContent=oldText;},1200);

}finally{

setTimeout(()=>{saveBtn.disabled=false;},300);

}

});

render();

}

document.addEventListener("DOMContentLoaded",function(){

initFollowButton();
initBannerEditor();

});

})();