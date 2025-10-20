import { initializeApp } from "https://www.gstatic.com/firebasejs/10.13.1/firebase-app.js";
import {
  getAuth,
  onAuthStateChanged,
  signInWithEmailAndPassword,
  signOut,
  createUserWithEmailAndPassword,
  updateProfile,
} from "https://www.gstatic.com/firebasejs/10.13.1/firebase-auth.js";

const firebaseConfig = {
  apiKey: "AIzaSyBZabUBc38cEJkjhgLFeuenlwzivKrlhfM",
  authDomain: "lagrangero-group4.firebaseapp.com",
  projectId: "lagrangero-group4",
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);

const memberGreetingEl = document.querySelector('[data-user-greeting]');
function showMember(on) {
  document.querySelectorAll('[data-member]').forEach((el) => {
    el.hidden = !on;
  });
  document.querySelectorAll('[data-guest]').forEach((el) => {
    el.hidden = on;
  });
}

showMember(false);

function updateGreeting(user) {
  if (!memberGreetingEl) return;
  if (!user) {
    memberGreetingEl.textContent = '';
    return;
  }
  const name = (user.displayName || '').trim() || (user.email ? user.email.split('@')[0] : '会員');
  memberGreetingEl.textContent = `${name}さんのマイページ`;
}

function renderMemberProbe(info) {
  const box = document.querySelector('#member-content');
  if (!box) return;
  if (!info) {
    box.textContent = '';
    return;
  }
  box.textContent = JSON.stringify(info, null, 2);
}

async function emitAuthState(user, idToken) {
  const detail = {
    user: user
      ? {
          uid: user.uid,
          email: user.email || null,
          displayName: user.displayName || null,
        }
      : null,
    idToken: idToken || null,
  };
  window.firebaseAuthState = detail;
  window.dispatchEvent(new CustomEvent('firebase-auth-state', { detail }));
}

async function handleAuthState(user) {
  if (!user) {
    showMember(false);
    updateGreeting(null);
    renderMemberProbe(null);
    await emitAuthState(null, null);
    return;
  }

  try {
    const idToken = await user.getIdToken();
    const res = await fetch('/api/member-only', {
      headers: {
        Authorization: `Bearer ${idToken}`,
      },
    });
    if (!res.ok) {
      showMember(false);
      updateGreeting(null);
      renderMemberProbe(null);
      await emitAuthState(null, null);
      return;
    }
    const data = await res.json().catch(() => null);
    showMember(true);
    updateGreeting(user);
    renderMemberProbe(data);
    await emitAuthState(user, idToken);
  } catch (error) {
    console.error('Failed to verify member session', error);
    showMember(false);
    updateGreeting(null);
    renderMemberProbe(null);
    await emitAuthState(null, null);
  }
}

onAuthStateChanged(auth, (user) => {
  handleAuthState(user);
});

export async function doLogin(email, password) {
  if (!email || !password) {
    throw new Error('email and password are required');
  }
  await signInWithEmailAndPassword(auth, email, password);
}

export async function doSignup(email, password, displayName) {
  if (!email || !password || !displayName) {
    throw new Error('displayName, email and password are required');
  }
  const cred = await createUserWithEmailAndPassword(auth, email, password);
  try {
    await updateProfile(cred.user, { displayName: displayName.trim() });
  } catch (error) {
    console.warn('Failed to update displayName', error);
  }
  return cred.user;
}

export async function doLogout() {
  if (!window.confirm('本当にログアウトしてもいいですか？')) {
    return;
  }
  await signOut(auth);
  window.alert('ログアウトしました。');
}

export function getCurrentUser() {
  return auth.currentUser;
}

export async function getCurrentIdToken(forceRefresh = false) {
  const user = auth.currentUser;
  if (!user) return null;
  return user.getIdToken(forceRefresh);
}

window.firebaseAuth = {
  auth,
  doLogin,
  doSignup,
  doLogout,
  getCurrentUser,
  getCurrentIdToken,
};

export { auth };

