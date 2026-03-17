let resultData = null;

async function process(){

let fileInput=document.getElementById("file");

if(fileInput.files.length===0){
alert("Upload CSV");
return;
}

let fd=new FormData();
fd.append("file",fileInput.files[0]);
fd.append("overrides","{}");

let res=await fetch("/process",{method:"POST",body:fd});
let data=await res.json();

if(data.error){
alert(data.error);
return;
}

resultData=data;

document.getElementById("stats").innerHTML=`
<p>Total rows: ${data.total_rows}</p>
<p>Matched SKUs: ${data.matched_rows}</p>
<p>List cost: ₹${data.total_list_cost.toFixed(2)}</p>
<p>After discount: ₹${data.total_after_discount.toFixed(2)}</p>
`;

renderPreview(data.preview);
}


function renderPreview(rows){

let table=document.getElementById("preview");

let headers=[
"Service description",
"SKU description",
"Usage amount",
"Usage unit",
"List cost",
"(₹)discount %",
"Discounted value",
"after Discount"
];

let html="<tr>";

headers.forEach(h=>{
html+=`<th>${h}</th>`;
});

html+="</tr>";

rows.forEach(r=>{

html+="<tr>";

headers.forEach(h=>{
html+=`<td>${r[h]}</td>`;
});

html+="</tr>";

});

table.innerHTML=html;

}