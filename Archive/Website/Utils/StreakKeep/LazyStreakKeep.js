// ==UserScript==
// @name         Lazy Streak Keeper
// @namespace    github.com/midweststatic/LazyStreakKeeper
// @version      1.4
// @author       Simon
// @description  A lightweight script to automatically keep your streak going, without even needing to open a DM.
// @match        *://www.kogama.com/*
// @run-at       document-idle
// @grant        none
// ==/UserScript==


// CONFIG
const USERID = '00000'; // EDIT THIS VALUE TO YOUR OWN UID
const TARGET_PROFILE = 670350173; // UID OF THE STREAK KEEPER ACCOUNT

const LAST_SENT_KEY = 'ls_last_sent';
const FOLLOW_STATE_KEY = 'ls_follow_state';

const MESSAGE_INTERVAL_MS = 7 * 60 * 60 * 1000; // INTERVAL OF THE MESSAGING, EACH 7 HOURS AS OF DEFAULT | 7 * 60 * 60 * 1000 |
const POLL_INTERVAL_MS = 60 * 1000;

const INITIAL_HISTORY_DELAY_MS = 1000;
const HISTORY_RETRY_DELAY_MS = 10 * 1000;
const RESPONSE_WAIT_MS = 3 * 60 * 1000;
const SECOND_RESPONSE_WAIT_MS = 60 * 1000;

const MESSAGES = [
  "you are so loved <3",
  "streak check in, hi!",
  "keeping the streak alive <3",
  "quick hello from your streak bot",
  "sending love and a tiny nudge",
  "streak maintained, hi!",
  "just popping in to keep things going",
  "cheering for the streak <3",
  "automated hello, have a nice day",
  "tiny reminder: you are awesome"
];

function nowIso(){ return new Date().toISOString(); }
function isoToTs(iso){ const t = Date.parse(iso||''); return Number.isFinite(t)?t:0; }
function getStorage(k){ try{ return localStorage.getItem(k) }catch(e){return null} }
function setStorage(k,v){ try{ localStorage.setItem(k,v) }catch(e){} }
function chooseRandom(a){ return a[Math.floor(Math.random()*a.length)] }
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)) }

async function postChat(message){
  const url = `https://www.kogama.com/chat/${USERID}/`;
  const resp = await fetch(url,{
    method:'POST',
    credentials:'include',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ to_profile_id: TARGET_PROFILE, message })
  });
  if(!resp.ok) throw new Error('POST failed '+resp.status);
  return resp.json().catch(()=>null);
}

async function fetchHistory(){
  const url = `https://www.kogama.com/chat/${USERID}/history/${TARGET_PROFILE}/`;
  const resp = await fetch(url,{ method:'GET', credentials:'include' });
  if(!resp.ok) throw new Error('HISTORY GET failed '+resp.status);
  return resp.json().catch(()=>null);
}

async function waitForReply(timeoutMs){
  const start = Date.now();
  await sleep(INITIAL_HISTORY_DELAY_MS);
  while(Date.now()-start < timeoutMs){
    try{
      const hist = await fetchHistory();
      if(hist && Array.isArray(hist.data) && hist.data.length>0){
        const latest = hist.data[0];
        if(latest && Number(latest.from_profile_id) === Number(TARGET_PROFILE)) return latest;
      }
    }catch(e){}
    const timeLeft = timeoutMs - (Date.now()-start);
    if(timeLeft<=0) break;
    await sleep(Math.min(HISTORY_RETRY_DELAY_MS, timeLeft));
  }
  return null;
}

async function sendIfDue(){
  const lastIso = getStorage(LAST_SENT_KEY) || '';
  const lastTs = isoToTs(lastIso);
  const now = Date.now();
  if(now - lastTs < MESSAGE_INTERVAL_MS) return;

  const followStateRaw = getStorage(FOLLOW_STATE_KEY) || '{}';
  let followState = {};
  try{ followState = JSON.parse(followStateRaw) }catch(e){ followState = {} }

  const initialMsg = chooseRandom(MESSAGES);
  try{
    await postChat(initialMsg);
    const sentIso = nowIso();
    setStorage(LAST_SENT_KEY, sentIso);
    followState = { cycleStart: sentIso, sentCount: 1 };
    setStorage(FOLLOW_STATE_KEY, JSON.stringify(followState));

    const reply = await waitForReply(RESPONSE_WAIT_MS);
    if(reply){
      const follow1 = chooseRandom(MESSAGES);
      await postChat(follow1);
      followState.sentCount = 2;
      followState.lastFollow = nowIso();
      setStorage(FOLLOW_STATE_KEY, JSON.stringify(followState));

      const reply2 = await waitForReply(SECOND_RESPONSE_WAIT_MS);
      if(reply2){
        const follow2 = chooseRandom(MESSAGES);
        await postChat(follow2);
        followState.sentCount = 3;
        followState.lastFollow = nowIso();
        setStorage(FOLLOW_STATE_KEY, JSON.stringify(followState));
      }
    }
  }catch(e){
    return;
  }
}

(function main(){
  sendIfDue();
  setInterval(sendIfDue, POLL_INTERVAL_MS);
})();
