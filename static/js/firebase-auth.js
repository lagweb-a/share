import { initializeApp } from "https://www.gstatic.com/firebasejs/10.13.1/firebase-app.js";
import { getAuth, onAuthStateChanged, signInWithEmailAndPassword, signOut } from "https://www.gstatic.com/firebasejs/10.13.1/firebase-auth.js";

const firebaseConfig = {
  apiKey: "AIzaSyBZabUBc38cEJkjhgLFeuenlwzivKrlhfM",
  authDomain: "lagrangero-group4.firebaseapp.com",
  projectId: "lagrangero-group4",
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);

window.doLogout = async function() {
  await signOut(auth);
  alert("ログアウトしました");
};

// 新規登録関数
window.signup = async () => {
  const email = prompt('メールアドレスを入力してください');
  const pass  = prompt('パスワード（6文字以上）を入力してください');
  try {
    const cred = await createUserWithEmailAndPassword(auth, email, pass);
    alert('会員登録が完了しました！ログイン状態になっています。');
  } catch (err) {
    alert('登録エラー: ' + err.message);
  }
};

function showMember(on) {
  document.querySelectorAll('[data-member]').forEach((el) => {
    el.hidden = !on;
  });
  document.querySelectorAll('[data-guest]').forEach((el) => {
    el.hidden = on;
  });
}

showMember(false);

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
      renderMemberProbe(null);
      await emitAuthState(null, null);
      return;
    }
    const data = await res.json().catch(() => null);
    showMember(true);
    renderMemberProbe(data);
    await emitAuthState(user, idToken);
  } catch (error) {
    console.error('Failed to verify member session', error);
    showMember(false);
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

export async function doLogout() {
  await signOut(auth);
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
  doLogout,
  getCurrentUser,
  getCurrentIdToken,
};

export { auth };

