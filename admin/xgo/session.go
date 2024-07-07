/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package xgo

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

/*Session会话管理*/
type SessionMgr struct {
	mCookieName  string       //客户端cookie名称
	mLock        sync.RWMutex //互斥(保证线程安全)
	mMaxLifeTime int64        //垃圾回收时间

	mSessions map[string]*Session //保存session的指针[sessionID] = session
}

// 创建会话管理器(cookieName:在浏览器中cookie的名字;maxLifeTime:最长生命周期)
func NewSessionMgr(cookieName string, maxLifeTime int64) *SessionMgr {
	mgr := &SessionMgr{mCookieName: cookieName, mMaxLifeTime: maxLifeTime, mSessions: make(map[string]*Session)}

	//启动定时回收
	go mgr.GC()

	return mgr
}

// 在开始页面登陆页面，开始Session
func (mgr *SessionMgr) StartSession(w http.ResponseWriter, r *http.Request) string {
	mgr.mLock.Lock()
	defer mgr.mLock.Unlock()

	//无论原来有没有，都重新创建一个新的session
	newSessionID := url.QueryEscape(mgr.NewSessionID())

	//存指针
	var session *Session = &Session{mSessionID: newSessionID, mLastTimeAccessed: time.Now(), mValues: make(map[interface{}]interface{})}
	mgr.mSessions[newSessionID] = session
	//让浏览器cookie设置过期时间
	cookie := http.Cookie{Name: mgr.mCookieName, Value: newSessionID, Path: "/", HttpOnly: true, MaxAge: int(mgr.mMaxLifeTime)}
	http.SetCookie(w, &cookie)

	return newSessionID
}

// 结束Session
func (mgr *SessionMgr) EndSession(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(mgr.mCookieName)
	if err != nil || cookie.Value == "" {
		return
	} else {
		mgr.mLock.Lock()
		defer mgr.mLock.Unlock()

		delete(mgr.mSessions, cookie.Value)

		//让浏览器cookie立刻过期
		expiration := time.Now()
		cookie := http.Cookie{Name: mgr.mCookieName, Path: "/", HttpOnly: true, Expires: expiration, MaxAge: -1}
		http.SetCookie(w, &cookie)
	}
}

// 结束session
func (mgr *SessionMgr) EndSessionBy(sessionID string) {
	mgr.mLock.Lock()
	defer mgr.mLock.Unlock()

	delete(mgr.mSessions, sessionID)
}

// 设置session里面的值
func (mgr *SessionMgr) SetSessionVal(sessionID string, key interface{}, value interface{}) {
	mgr.mLock.Lock()
	defer mgr.mLock.Unlock()

	if session, ok := mgr.mSessions[sessionID]; ok {
		session.mValues[key] = value
	}
}

// 得到session里面的值
func (mgr *SessionMgr) GetSessionVal(sessionID string, key interface{}) (interface{}, bool) {
	mgr.mLock.RLock()
	defer mgr.mLock.RUnlock()

	if session, ok := mgr.mSessions[sessionID]; ok {
		if val, ok := session.mValues[key]; ok {
			return val, ok
		}
	}

	return nil, false
}

// 得到sessionID列表
func (mgr *SessionMgr) GetSessionIDList() []string {
	mgr.mLock.RLock()
	defer mgr.mLock.RUnlock()

	sessionIDList := make([]string, 0)

	for k, _ := range mgr.mSessions {
		sessionIDList = append(sessionIDList, k)
	}

	return sessionIDList[0:len(sessionIDList)]
}

// 判断Cookie的合法性（每进入一个页面都需要判断合法性）
func (mgr *SessionMgr) CheckCookieValid(w http.ResponseWriter, r *http.Request) string {
	var cookie, err = r.Cookie(mgr.mCookieName)

	if cookie == nil ||
		err != nil {
		return ""
	}

	mgr.mLock.Lock()
	defer mgr.mLock.Unlock()

	sessionID := cookie.Value

	if session, ok := mgr.mSessions[sessionID]; ok {
		session.mLastTimeAccessed = time.Now() //判断合法性的同时，更新最后的访问时间
		return sessionID
	}

	return ""
}

// 更新最后访问时间
func (mgr *SessionMgr) GetLastAccessTime(sessionID string) time.Time {
	mgr.mLock.RLock()
	defer mgr.mLock.RUnlock()

	if session, ok := mgr.mSessions[sessionID]; ok {
		return session.mLastTimeAccessed
	}

	return time.Now()
}

// GC回收
func (mgr *SessionMgr) GC() {
	mgr.mLock.Lock()
	defer mgr.mLock.Unlock()

	for sessionID, session := range mgr.mSessions {
		//删除超过时限的session
		if session.mLastTimeAccessed.Unix()+mgr.mMaxLifeTime < time.Now().Unix() {
			delete(mgr.mSessions, sessionID)
		}
	}

	//定时回收
	time.AfterFunc(time.Duration(mgr.mMaxLifeTime)*time.Second, func() { mgr.GC() })
}

// 创建唯一ID
func (mgr *SessionMgr) NewSessionID() string {
	b := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		nano := time.Now().UnixNano() //微秒
		return strconv.FormatInt(nano, 10)
	}
	return base64.URLEncoding.EncodeToString(b)
}

//——————————————————————————
/*会话*/
type Session struct {
	mSessionID        string                      //唯一id
	mLastTimeAccessed time.Time                   //最后访问时间
	mValues           map[interface{}]interface{} //其它对应值(保存用户所对应的一些值，比如用户权限之类)
}
