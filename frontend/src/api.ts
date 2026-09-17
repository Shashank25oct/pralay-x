const API = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1';

async function request(path: string, options: RequestInit = {}) {
  const token = localStorage.getItem('pralay_token');
  const headers: Record<string,string> = {'Content-Type':'application/json', ...(options.headers as Record<string,string> || {})};
  if (token) headers.Authorization = `Bearer ${token}`;
  const r = await fetch(`${API}${path}`, {...options, headers});
  if (!r.ok) throw new Error((await r.json().catch(()=>({detail:'Request failed'}))).detail || 'Request failed');
  return r.json();
}
export const api = {
  publicDashboard: ()=>request('/public/dashboard'),
  authorityDashboard: ()=>request('/dashboard/overview'),
  login: (user_id:string,password:string)=>request('/auth/login',{method:'POST',body:JSON.stringify({user_id,password})}),
  logout: ()=>request('/auth/logout',{method:'POST'}),
  complaint: (body:any)=>request('/citizen-reports',{method:'POST',body:JSON.stringify(body)}),
  updateComplaint: (id:string,status:string)=>request(`/citizen-reports/${id}`,{method:'PATCH',body:JSON.stringify({status})}),
  runSimulation: (body:any)=>request('/simulation/run',{method:'POST',body:JSON.stringify(body)}),
  updateIncident: (id:string,status:string)=>request(`/incidents/${id}`,{method:'PATCH',body:JSON.stringify({status})}),
  report: (state='ALL INDIA')=>request(`/reports/situation?state=${encodeURIComponent(state)}`),
  reportPdf: async (state='ALL INDIA')=>{ const token=localStorage.getItem('pralay_token'); const r=await fetch(`${API}/reports/situation/pdf?state=${encodeURIComponent(state)}`,{headers: token?{Authorization:`Bearer ${token}`}:{}}); if(!r.ok) throw new Error('Unable to generate PDF report.'); const blob=await r.blob(); const url=URL.createObjectURL(blob); const a=document.createElement('a'); a.href=url; a.download=`PRALAY-X-Situation-Report.pdf`; document.body.appendChild(a); a.click(); a.remove(); setTimeout(()=>URL.revokeObjectURL(url),1500); },
  reportIncident: (body:any)=>request('/incidents',{method:'POST',body:JSON.stringify(body)}),
  createAlert: (body:any)=>request('/alerts',{method:'POST',body:JSON.stringify(body)}),
  updateAlert: (id:number,active:boolean)=>request(`/alerts/${id}`,{method:'PATCH',body:JSON.stringify({active})}),
  dispatchResource: (body:any)=>request('/resources/dispatch',{method:'POST',body:JSON.stringify(body)}),
  resources: ()=>request('/resources'),
  updateResource: (id:string,status:string)=>request(`/resources/dispatch/${id}`,{method:'PATCH',body:JSON.stringify({status})}),
  recommendations: ()=>request('/ai/recommendations'),
  evacuations: ()=>request('/evacuations'),
  createEvacuation: (body:any)=>request('/evacuations',{method:'POST',body:JSON.stringify(body)}),
  updateEvacuation: (id:string,status:string)=>request(`/evacuations/${id}`,{method:'PATCH',body:JSON.stringify({status})}),
  routes: ()=>request('/routes'),
  createRoute: (body:any)=>request('/routes',{method:'POST',body:JSON.stringify(body)}),
  addData: (body:any)=>request('/data-entry',{method:'POST',body:JSON.stringify(body)}),
  broadcast: (body:any)=>request('/communication/broadcast',{method:'POST',body:JSON.stringify(body)}),
  analytics: (state='ALL INDIA')=>request(`/workspace/analytics?state=${encodeURIComponent(state)}`),
  search: (q:string,state='ALL INDIA',category='ALL')=>request(`/workspace/search?q=${encodeURIComponent(q)}&state=${encodeURIComponent(state)}&category=${encodeURIComponent(category)}`),
  ask: (message:string,state='ALL INDIA',session_id='')=>request('/workspace/ask',{method:'POST',body:JSON.stringify({message,state,session_id})}),
  chatHistory: (session_id:string,state='ALL INDIA')=>request(`/workspace/chat-history?session_id=${encodeURIComponent(session_id)}&state=${encodeURIComponent(state)}`),
  operationalPlan: (state='ALL INDIA',incident_id='')=>request(`/workspace/operational-plan?state=${encodeURIComponent(state)}&incident_id=${encodeURIComponent(incident_id)}`),
  publicOperationalPlan: (state='ALL INDIA',incident_id='')=>request(`/public/operational-plan?state=${encodeURIComponent(state)}&incident_id=${encodeURIComponent(incident_id)}`),
  publicAsk: (message:string,state='ALL INDIA',session_id='')=>request('/public/ai/ask',{method:'POST',body:JSON.stringify({message,state,session_id})}),
  publicChatHistory: (session_id:string)=>request(`/public/ai/history?session_id=${encodeURIComponent(session_id)}`),
  forecast: (state='ALL INDIA')=>request(`/workspace/forecast?state=${encodeURIComponent(state)}`),
  impact: (state='ALL INDIA')=>request(`/workspace/impact?state=${encodeURIComponent(state)}`),
  historical: (state='ALL INDIA')=>request(`/workspace/historical?state=${encodeURIComponent(state)}`),
  compare: (a:string,b:string)=>request(`/workspace/compare?state_a=${encodeURIComponent(a)}&state_b=${encodeURIComponent(b)}`),
  communication: (state='ALL INDIA')=>request(`/workspace/communication?state=${encodeURIComponent(state)}`),
  admin: ()=>request('/workspace/admin'),
  resetSimulation: ()=>request('/simulation/reset',{method:'POST'}),
};
export const eventsUrl = `${API}/sse`;
