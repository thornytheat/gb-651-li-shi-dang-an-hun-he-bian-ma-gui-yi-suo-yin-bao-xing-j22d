const state={busy:false};
document.querySelectorAll("[data-endpoint]").forEach(btn=>btn.addEventListener("click",async()=>{
  if(state.busy)return; state.busy=true; const out=btn.parentElement.querySelector(".result"); if(out)out.textContent="运行中";
  const isGet=btn.dataset.endpoint.includes("export");
  try{
    const res=await fetch(btn.dataset.endpoint,{method:isGet?"GET":"POST",headers:{"content-type":"application/json"},body:isGet?null:(btn.dataset.body||"{}")});
    const text=await res.text();
    if("reload" in btn.dataset){location.reload();return}
    if(out)out.textContent=text;
  }catch(e){if(out)out.textContent="请求失败，请重试"}finally{state.busy=false}
}));
document.querySelector("#refresh")?.addEventListener("click",()=>location.reload());
