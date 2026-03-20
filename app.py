import sys, os, threading, time

# ── Persistent paths (survives .exe restarts) ─────────────
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH    = os.path.join(BASE_DIR, 'social.db')
UPLOAD_DIR = os.path.join(BASE_DIR, 'static', 'uploads')
os.makedirs(UPLOAD_DIR, exist_ok=True)

from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
from flask_socketio import SocketIO, emit, join_room, leave_room
from werkzeug.security import generate_password_hash, check_password_hash
import sqlite3, datetime, uuid, re

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'pulse-super-secret-2024')
app.config['UPLOAD_FOLDER'] = UPLOAD_DIR
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

OWNER_USERNAME  = 'gabcius'
ALLOWED_IMG     = {'png','jpg','jpeg','gif','webp'}
ALLOWED_VIDEO   = {'mp4','webm','mov'}
ALLOWED_ALL     = ALLOWED_IMG | ALLOWED_VIDEO

def allowed_file(filename, types=ALLOWED_ALL):
    return '.' in filename and filename.rsplit('.',1)[1].lower() in types

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db

def init_db():
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            avatar TEXT DEFAULT 'default.png',
            cover TEXT DEFAULT NULL,
            bio TEXT DEFAULT '',
            role TEXT DEFAULT 'user',
            is_banned INTEGER DEFAULT 0,
            theme TEXT DEFAULT 'dark',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            image TEXT DEFAULT NULL,
            video TEXT DEFAULT NULL,
            post_type TEXT DEFAULT 'post',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS stories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            media TEXT NOT NULL,
            media_type TEXT DEFAULT 'image',
            caption TEXT DEFAULT '',
            expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS reels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            video TEXT NOT NULL,
            caption TEXT DEFAULT '',
            audio_name TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS hashtags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT UNIQUE NOT NULL,
            count INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS post_hashtags (
            post_id INTEGER,
            hashtag_id INTEGER,
            PRIMARY KEY (post_id, hashtag_id)
        );
        CREATE TABLE IF NOT EXISTS likes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            post_id INTEGER,
            reel_id INTEGER,
            UNIQUE(user_id, post_id),
            UNIQUE(user_id, reel_id)
        );
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            post_id INTEGER,
            reel_id INTEGER,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS friendships (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            requester_id INTEGER NOT NULL,
            addressee_id INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(requester_id, addressee_id)
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id INTEGER NOT NULL,
            receiver_id INTEGER NOT NULL,
            content TEXT DEFAULT '',
            media TEXT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_read INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            avatar TEXT DEFAULT 'default.png',
            owner_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS group_members (
            group_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT DEFAULT 'member',
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (group_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS group_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            image TEXT DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            from_user_id INTEGER,
            type TEXT NOT NULL,
            content TEXT NOT NULL,
            link TEXT DEFAULT '',
            is_read INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS livestreams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            room TEXT UNIQUE NOT NULL,
            is_live INTEGER DEFAULT 1,
            viewers INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS story_views (
            story_id INTEGER,
            user_id INTEGER,
            PRIMARY KEY (story_id, user_id)
        );
    ''')
    for col in [("role","TEXT DEFAULT 'user'"),("is_banned","INTEGER DEFAULT 0"),
                ("theme","TEXT DEFAULT 'dark'"),("cover","TEXT DEFAULT NULL")]:
        try: db.execute(f"ALTER TABLE users ADD COLUMN {col[0]} {col[1]}")
        except: pass
    db.execute("UPDATE users SET role='owner' WHERE username=?", (OWNER_USERNAME,))
    db.commit()
    db.close()

def add_notification(user_id, from_user_id, ntype, content, link=''):
    if user_id == from_user_id: return
    db = get_db()
    db.execute('INSERT INTO notifications (user_id,from_user_id,type,content,link) VALUES (?,?,?,?,?)',
               (user_id, from_user_id, ntype, content, link))
    db.commit()
    db.close()
    socketio.emit('notification', {'content': content, 'link': link}, room=f'user_{user_id}')

def extract_hashtags(text):
    return list(set(re.findall(r'#(\w+)', text.lower())))

def process_hashtags(db, post_id, content):
    tags = extract_hashtags(content)
    for tag in tags:
        db.execute('INSERT INTO hashtags (tag,count) VALUES (?,1) ON CONFLICT(tag) DO UPDATE SET count=count+1', (tag,))
        row = db.execute('SELECT id FROM hashtags WHERE tag=?', (tag,)).fetchone()
        try: db.execute('INSERT INTO post_hashtags (post_id,hashtag_id) VALUES (?,?)', (post_id, row['id']))
        except: pass

def login_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session: return redirect(url_for('login'))
        db = get_db()
        u = db.execute('SELECT is_banned FROM users WHERE id=?', (session['user_id'],)).fetchone()
        db.close()
        if u and u['is_banned']:
            session.clear(); flash('Your account has been banned.','error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def owner_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session: return redirect(url_for('login'))
        db = get_db()
        u = db.execute('SELECT role FROM users WHERE id=?', (session['user_id'],)).fetchone()
        db.close()
        if not u or u['role'] != 'owner': flash('Owner access required.','error'); return redirect(url_for('feed'))
        return f(*args, **kwargs)
    return decorated

def get_current_user():
    if 'user_id' not in session: return None
    db = get_db(); u = db.execute('SELECT * FROM users WHERE id=?', (session['user_id'],)).fetchone(); db.close(); return u

def is_owner():
    if 'user_id' not in session: return False
    db = get_db(); u = db.execute('SELECT role FROM users WHERE id=?', (session['user_id'],)).fetchone(); db.close()
    return u and u['role'] == 'owner'

def save_file(file, allowed):
    if file and file.filename and allowed_file(file.filename, allowed):
        ext = file.filename.rsplit('.',1)[1].lower()
        fn  = str(uuid.uuid4()) + '.' + ext
        file.save(os.path.join(UPLOAD_DIR, fn))
        return fn
    return None

# ── Keep-alive (for Render free tier) ────────────────────
def keep_alive():
    time.sleep(60)
    url = os.environ.get('RENDER_EXTERNAL_URL','')
    if url:
        while True:
            time.sleep(600)
            try:
                import urllib.request
                urllib.request.urlopen(url)
            except: pass

threading.Thread(target=keep_alive, daemon=True).start()

# ── Static uploads ────────────────────────────────────────
@app.route('/uploads/<filename>')
def uploaded_file(filename):
    from flask import send_from_directory
    return send_from_directory(UPLOAD_DIR, filename)

# ── Auth ──────────────────────────────────────────────────
@app.route('/')
def index():
    return redirect(url_for('feed') if 'user_id' in session else url_for('login'))

@app.route('/register', methods=['GET','POST'])
def register():
    if request.method == 'POST':
        u = request.form['username'].strip()
        e = request.form['email'].strip()
        p = request.form['password']
        db = get_db()
        try:
            role = 'owner' if u == OWNER_USERNAME else 'user'
            db.execute('INSERT INTO users (username,email,password,role) VALUES (?,?,?,?)',
                       (u, e, generate_password_hash(p), role))
            db.commit()
            user = db.execute('SELECT * FROM users WHERE username=?', (u,)).fetchone()
            session['user_id'] = user['id']; session['username'] = user['username']
            session['theme']   = user['theme']
            return redirect(url_for('feed'))
        except sqlite3.IntegrityError: flash('Username or email already exists.','error')
        finally: db.close()
    return render_template('auth.html', mode='register')

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        u  = request.form['username'].strip()
        p  = request.form['password']
        db = get_db()
        user = db.execute('SELECT * FROM users WHERE username=?', (u,)).fetchone()
        db.close()
        if user and user['is_banned']: flash('Your account has been banned.','error'); return render_template('auth.html',mode='login')
        if user and check_password_hash(user['password'], p):
            session['user_id'] = user['id']; session['username'] = user['username']
            session['theme']   = user['theme']
            return redirect(url_for('feed'))
        flash('Invalid credentials.','error')
    return render_template('auth.html', mode='login')

@app.route('/logout')
def logout():
    session.clear(); return redirect(url_for('login'))

@app.route('/theme/toggle', methods=['POST'])
@login_required
def toggle_theme():
    db = get_db()
    u  = db.execute('SELECT theme FROM users WHERE id=?', (session['user_id'],)).fetchone()
    new_theme = 'light' if u['theme'] == 'dark' else 'dark'
    db.execute('UPDATE users SET theme=? WHERE id=?', (new_theme, session['user_id']))
    db.commit(); db.close()
    session['theme'] = new_theme
    return jsonify({'theme': new_theme})

# ── Feed ──────────────────────────────────────────────────
@app.route('/feed')
@login_required
def feed():
    db   = get_db()
    user = get_current_user()
    # Delete expired stories
    db.execute("DELETE FROM stories WHERE expires_at < datetime('now')")
    db.commit()
    posts = db.execute('''
        SELECT p.*, u.username, u.avatar, u.role,
               (SELECT COUNT(*) FROM likes WHERE post_id=p.id) as like_count,
               (SELECT COUNT(*) FROM likes WHERE post_id=p.id AND user_id=?) as user_liked,
               (SELECT COUNT(*) FROM comments WHERE post_id=p.id) as comment_count
        FROM posts p JOIN users u ON p.user_id=u.id
        WHERE p.post_type='post' AND (p.user_id=? OR p.user_id IN (
            SELECT CASE WHEN requester_id=? THEN addressee_id ELSE requester_id END
            FROM friendships WHERE (requester_id=? OR addressee_id=?) AND status='accepted'
        ))
        ORDER BY p.created_at DESC LIMIT 50
    ''', (session['user_id'],)*5).fetchall()
    # Stories (friends + self, not expired)
    stories = db.execute('''
        SELECT s.*, u.username, u.avatar,
               (SELECT COUNT(*) FROM story_views WHERE story_id=s.id) as view_count,
               (SELECT COUNT(*) FROM story_views WHERE story_id=s.id AND user_id=?) as viewed
        FROM stories s JOIN users u ON s.user_id=u.id
        WHERE s.user_id=? OR s.user_id IN (
            SELECT CASE WHEN requester_id=? THEN addressee_id ELSE requester_id END
            FROM friendships WHERE (requester_id=? OR addressee_id=?) AND status='accepted'
        )
        ORDER BY s.created_at DESC
    ''', (session['user_id'],)*5).fetchall()
    # Trending hashtags
    trending = db.execute('SELECT * FROM hashtags ORDER BY count DESC LIMIT 10').fetchall()
    requests_count = db.execute(
        'SELECT COUNT(*) as cnt FROM friendships WHERE addressee_id=? AND status="pending"',
        (session['user_id'],)).fetchone()['cnt']
    notif_count = db.execute(
        'SELECT COUNT(*) as cnt FROM notifications WHERE user_id=? AND is_read=0',
        (session['user_id'],)).fetchone()['cnt']
    suggested = db.execute('''
        SELECT * FROM users WHERE id!=? AND is_banned=0 AND id NOT IN (
            SELECT CASE WHEN requester_id=? THEN addressee_id ELSE requester_id END
            FROM friendships WHERE requester_id=? OR addressee_id=?
        ) LIMIT 5
    ''', (session['user_id'],)*4).fetchall()
    livestreams = db.execute('''
        SELECT l.*, u.username, u.avatar FROM livestreams l
        JOIN users u ON l.user_id=u.id WHERE l.is_live=1 ORDER BY l.created_at DESC LIMIT 5
    ''').fetchall()
    db.close()
    return render_template('feed.html', posts=posts, user=user, stories=stories,
                           trending=trending, requests_count=requests_count,
                           notif_count=notif_count, suggested=suggested,
                           livestreams=livestreams, owner=is_owner())

# ── Posts ─────────────────────────────────────────────────
@app.route('/post/create', methods=['POST'])
@login_required
def create_post():
    content = request.form.get('content','').strip()
    img = save_file(request.files.get('image'), ALLOWED_IMG)
    vid = save_file(request.files.get('video'), ALLOWED_VIDEO)
    if content or img or vid:
        db = get_db()
        db.execute('INSERT INTO posts (user_id,content,image,video,post_type) VALUES (?,?,?,?,?)',
                   (session['user_id'], content, img, vid, 'post'))
        post_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
        if content: process_hashtags(db, post_id, content)
        db.commit(); db.close()
    return redirect(url_for('feed'))

@app.route('/post/<int:post_id>/delete', methods=['POST'])
@login_required
def delete_post(post_id):
    db = get_db()
    if is_owner(): db.execute('DELETE FROM posts WHERE id=?', (post_id,))
    else:          db.execute('DELETE FROM posts WHERE id=? AND user_id=?', (post_id, session['user_id']))
    db.commit(); db.close()
    return redirect(request.referrer or url_for('feed'))

@app.route('/post/<int:post_id>/like', methods=['POST'])
@login_required
def like_post(post_id):
    db  = get_db()
    ex  = db.execute('SELECT * FROM likes WHERE user_id=? AND post_id=?', (session['user_id'], post_id)).fetchone()
    if ex:
        db.execute('DELETE FROM likes WHERE user_id=? AND post_id=?', (session['user_id'], post_id)); liked=False
    else:
        db.execute('INSERT OR IGNORE INTO likes (user_id,post_id) VALUES (?,?)', (session['user_id'], post_id)); liked=True
        post = db.execute('SELECT user_id,content FROM posts WHERE id=?', (post_id,)).fetchone()
        if post: add_notification(post['user_id'], session['user_id'], 'like',
                                  f'{session["username"]} liked your post', f'/post/{post_id}')
    db.commit()
    count = db.execute('SELECT COUNT(*) as c FROM likes WHERE post_id=?', (post_id,)).fetchone()['c']
    db.close()
    return jsonify({'liked': liked, 'count': count})

@app.route('/post/<int:post_id>/comment', methods=['POST'])
@login_required
def comment_post(post_id):
    content = request.form.get('content','').strip()
    if content:
        db = get_db()
        db.execute('INSERT INTO comments (user_id,post_id,content) VALUES (?,?,?)',
                   (session['user_id'], post_id, content))
        post = db.execute('SELECT user_id FROM posts WHERE id=?', (post_id,)).fetchone()
        if post: add_notification(post['user_id'], session['user_id'], 'comment',
                                  f'{session["username"]} commented on your post', f'/post/{post_id}')
        db.commit(); db.close()
    return redirect(request.referrer or url_for('feed'))

@app.route('/post/<int:post_id>')
@login_required
def view_post(post_id):
    db   = get_db(); user = get_current_user()
    post = db.execute('''
        SELECT p.*, u.username, u.avatar, u.role,
               (SELECT COUNT(*) FROM likes WHERE post_id=p.id) as like_count,
               (SELECT COUNT(*) FROM likes WHERE post_id=p.id AND user_id=?) as user_liked
        FROM posts p JOIN users u ON p.user_id=u.id WHERE p.id=?
    ''', (session['user_id'], post_id)).fetchone()
    comments = db.execute('''
        SELECT c.*, u.username, u.avatar, u.role FROM comments c
        JOIN users u ON c.user_id=u.id WHERE c.post_id=? ORDER BY c.created_at ASC
    ''', (post_id,)).fetchall()
    db.close()
    return render_template('post.html', post=post, comments=comments, user=user, owner=is_owner())

# ── Stories ───────────────────────────────────────────────
@app.route('/story/create', methods=['POST'])
@login_required
def create_story():
    caption = request.form.get('caption','').strip()
    media   = save_file(request.files.get('media'), ALLOWED_ALL)
    if media:
        ext        = media.rsplit('.',1)[1].lower()
        media_type = 'video' if ext in ALLOWED_VIDEO else 'image'
        expires    = (datetime.datetime.now() + datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        db = get_db()
        db.execute('INSERT INTO stories (user_id,media,media_type,caption,expires_at) VALUES (?,?,?,?,?)',
                   (session['user_id'], media, media_type, caption, expires))
        db.commit(); db.close()
    return redirect(url_for('feed'))

@app.route('/story/<int:story_id>/view', methods=['POST'])
@login_required
def view_story(story_id):
    db = get_db()
    try: db.execute('INSERT INTO story_views (story_id,user_id) VALUES (?,?)', (story_id, session['user_id']))
    except: pass
    db.commit(); db.close()
    return jsonify({'ok': True})

# ── Reels ─────────────────────────────────────────────────
@app.route('/reels')
@login_required
def reels():
    db   = get_db(); user = get_current_user()
    reels_list = db.execute('''
        SELECT r.*, u.username, u.avatar, u.role,
               (SELECT COUNT(*) FROM likes WHERE reel_id=r.id) as like_count,
               (SELECT COUNT(*) FROM likes WHERE reel_id=r.id AND user_id=?) as user_liked,
               (SELECT COUNT(*) FROM comments WHERE reel_id=r.id) as comment_count
        FROM reels r JOIN users u ON r.user_id=u.id ORDER BY r.created_at DESC LIMIT 30
    ''', (session['user_id'],)).fetchall()
    notif_count = db.execute('SELECT COUNT(*) as cnt FROM notifications WHERE user_id=? AND is_read=0', (session['user_id'],)).fetchone()['cnt']
    db.close()
    return render_template('reels.html', reels=reels_list, user=user, notif_count=notif_count, owner=is_owner())

@app.route('/reel/create', methods=['POST'])
@login_required
def create_reel():
    caption    = request.form.get('caption','').strip()
    audio_name = request.form.get('audio_name','').strip()
    video      = save_file(request.files.get('video'), ALLOWED_VIDEO)
    if video:
        db = get_db()
        db.execute('INSERT INTO reels (user_id,video,caption,audio_name) VALUES (?,?,?,?)',
                   (session['user_id'], video, caption, audio_name))
        db.commit(); db.close()
    return redirect(url_for('reels'))

@app.route('/reel/<int:reel_id>/like', methods=['POST'])
@login_required
def like_reel(reel_id):
    db = get_db()
    ex = db.execute('SELECT * FROM likes WHERE user_id=? AND reel_id=?', (session['user_id'], reel_id)).fetchone()
    if ex:
        db.execute('DELETE FROM likes WHERE user_id=? AND reel_id=?', (session['user_id'], reel_id)); liked=False
    else:
        db.execute('INSERT OR IGNORE INTO likes (user_id,reel_id) VALUES (?,?)', (session['user_id'], reel_id)); liked=True
    db.commit()
    count = db.execute('SELECT COUNT(*) as c FROM likes WHERE reel_id=?', (reel_id,)).fetchone()['c']
    db.close()
    return jsonify({'liked': liked, 'count': count})

# ── Hashtags ──────────────────────────────────────────────
@app.route('/hashtag/<tag>')
@login_required
def hashtag(tag):
    db   = get_db(); user = get_current_user()
    posts = db.execute('''
        SELECT p.*, u.username, u.avatar, u.role,
               (SELECT COUNT(*) FROM likes WHERE post_id=p.id) as like_count,
               (SELECT COUNT(*) FROM likes WHERE post_id=p.id AND user_id=?) as user_liked,
               (SELECT COUNT(*) FROM comments WHERE post_id=p.id) as comment_count
        FROM posts p JOIN users u ON p.user_id=u.id
        JOIN post_hashtags ph ON ph.post_id=p.id
        JOIN hashtags h ON h.id=ph.hashtag_id
        WHERE h.tag=? ORDER BY p.created_at DESC
    ''', (session['user_id'], tag.lower())).fetchall()
    count = db.execute('SELECT count FROM hashtags WHERE tag=?', (tag.lower(),)).fetchone()
    db.close()
    return render_template('hashtag.html', tag=tag, posts=posts, user=user,
                           count=count['count'] if count else 0, owner=is_owner())

# ── Notifications ─────────────────────────────────────────
@app.route('/notifications')
@login_required
def notifications():
    db   = get_db(); user = get_current_user()
    notifs = db.execute('''
        SELECT n.*, u.username, u.avatar FROM notifications n
        LEFT JOIN users u ON n.from_user_id=u.id
        WHERE n.user_id=? ORDER BY n.created_at DESC LIMIT 50
    ''', (session['user_id'],)).fetchall()
    db.execute('UPDATE notifications SET is_read=1 WHERE user_id=?', (session['user_id'],))
    db.commit(); db.close()
    return render_template('notifications.html', notifs=notifs, user=user, notif_count=0, owner=is_owner())

@app.route('/notifications/count')
@login_required
def notif_count():
    db    = get_db()
    count = db.execute('SELECT COUNT(*) as c FROM notifications WHERE user_id=? AND is_read=0', (session['user_id'],)).fetchone()['c']
    db.close()
    return jsonify({'count': count})

# ── Groups ────────────────────────────────────────────────
@app.route('/groups')
@login_required
def groups():
    db   = get_db(); user = get_current_user()
    my_groups = db.execute('''
        SELECT g.*, (SELECT COUNT(*) FROM group_members WHERE group_id=g.id) as member_count
        FROM groups g JOIN group_members gm ON gm.group_id=g.id
        WHERE gm.user_id=? ORDER BY g.created_at DESC
    ''', (session['user_id'],)).fetchall()
    all_groups = db.execute('''
        SELECT g.*, (SELECT COUNT(*) FROM group_members WHERE group_id=g.id) as member_count,
               (SELECT 1 FROM group_members WHERE group_id=g.id AND user_id=?) as is_member
        FROM groups g ORDER BY member_count DESC LIMIT 20
    ''', (session['user_id'],)).fetchall()
    notif_count = db.execute('SELECT COUNT(*) as cnt FROM notifications WHERE user_id=? AND is_read=0', (session['user_id'],)).fetchone()['cnt']
    db.close()
    return render_template('groups.html', my_groups=my_groups, all_groups=all_groups,
                           user=user, notif_count=notif_count, owner=is_owner())

@app.route('/group/create', methods=['POST'])
@login_required
def create_group():
    name = request.form.get('name','').strip()
    desc = request.form.get('description','').strip()
    if name:
        db  = get_db()
        db.execute('INSERT INTO groups (name,description,owner_id) VALUES (?,?,?)',
                   (name, desc, session['user_id']))
        gid = db.execute('SELECT last_insert_rowid()').fetchone()[0]
        db.execute('INSERT INTO group_members (group_id,user_id,role) VALUES (?,?,?)',
                   (gid, session['user_id'], 'owner'))
        db.commit(); db.close()
    return redirect(url_for('groups'))

@app.route('/group/<int:group_id>')
@login_required
def view_group(group_id):
    db    = get_db(); user = get_current_user()
    group = db.execute('SELECT * FROM groups WHERE id=?', (group_id,)).fetchone()
    posts = db.execute('''
        SELECT gp.*, u.username, u.avatar, u.role FROM group_posts gp
        JOIN users u ON gp.user_id=u.id WHERE gp.group_id=? ORDER BY gp.created_at DESC
    ''', (group_id,)).fetchall()
    members = db.execute('''
        SELECT u.*, gm.role FROM users u JOIN group_members gm ON gm.user_id=u.id
        WHERE gm.group_id=? ORDER BY gm.role DESC
    ''', (group_id,)).fetchall()
    is_member = db.execute('SELECT 1 FROM group_members WHERE group_id=? AND user_id=?',
                           (group_id, session['user_id'])).fetchone()
    notif_count = db.execute('SELECT COUNT(*) as cnt FROM notifications WHERE user_id=? AND is_read=0', (session['user_id'],)).fetchone()['cnt']
    db.close()
    return render_template('group.html', group=group, posts=posts, members=members,
                           is_member=is_member, user=user, notif_count=notif_count, owner=is_owner())

@app.route('/group/<int:group_id>/join', methods=['POST'])
@login_required
def join_group(group_id):
    db = get_db()
    try: db.execute('INSERT INTO group_members (group_id,user_id) VALUES (?,?)', (group_id, session['user_id']))
    except: pass
    db.commit(); db.close()
    return redirect(url_for('view_group', group_id=group_id))

@app.route('/group/<int:group_id>/post', methods=['POST'])
@login_required
def group_post(group_id):
    content = request.form.get('content','').strip()
    img     = save_file(request.files.get('image'), ALLOWED_IMG)
    is_member = get_db().execute('SELECT 1 FROM group_members WHERE group_id=? AND user_id=?',
                                  (group_id, session['user_id'])).fetchone()
    if content and is_member:
        db = get_db()
        db.execute('INSERT INTO group_posts (group_id,user_id,content,image) VALUES (?,?,?,?)',
                   (group_id, session['user_id'], content, img))
        db.commit(); db.close()
    return redirect(url_for('view_group', group_id=group_id))

# ── Livestream ────────────────────────────────────────────
@app.route('/live')
@login_required
def live_list():
    db   = get_db(); user = get_current_user()
    streams = db.execute('''
        SELECT l.*, u.username, u.avatar FROM livestreams l
        JOIN users u ON l.user_id=u.id WHERE l.is_live=1 ORDER BY l.viewers DESC
    ''').fetchall()
    notif_count = db.execute('SELECT COUNT(*) as cnt FROM notifications WHERE user_id=? AND is_read=0', (session['user_id'],)).fetchone()['cnt']
    db.close()
    return render_template('live_list.html', streams=streams, user=user, notif_count=notif_count, owner=is_owner())

@app.route('/live/start', methods=['POST'])
@login_required
def start_live():
    title = request.form.get('title','Live Stream').strip()
    room  = str(uuid.uuid4())[:8]
    db    = get_db()
    db.execute('UPDATE livestreams SET is_live=0 WHERE user_id=?', (session['user_id'],))
    db.execute('INSERT INTO livestreams (user_id,title,room) VALUES (?,?,?)',
               (session['user_id'], title, room))
    db.commit(); db.close()
    return redirect(url_for('live_room', room=room))

@app.route('/live/<room>')
@login_required
def live_room(room):
    db     = get_db(); user = get_current_user()
    stream = db.execute('SELECT l.*,u.username,u.avatar FROM livestreams l JOIN users u ON l.user_id=u.id WHERE l.room=?', (room,)).fetchone()
    db.close()
    if not stream: return redirect(url_for('live_list'))
    is_broadcaster = stream['user_id'] == session['user_id']
    return render_template('live_room.html', stream=stream, user=user,
                           is_broadcaster=is_broadcaster, owner=is_owner())

@app.route('/live/<room>/end', methods=['POST'])
@login_required
def end_live(room):
    db = get_db()
    db.execute('UPDATE livestreams SET is_live=0 WHERE room=? AND user_id=?', (room, session['user_id']))
    db.commit(); db.close()
    return redirect(url_for('feed'))

# ── Profile ───────────────────────────────────────────────
@app.route('/profile/<username>')
@login_required
def profile(username):
    db           = get_db(); current = get_current_user()
    profile_user = db.execute('SELECT * FROM users WHERE username=?', (username,)).fetchone()
    if not profile_user: return "User not found", 404
    posts = db.execute('''
        SELECT p.*, u.username, u.avatar, u.role,
               (SELECT COUNT(*) FROM likes WHERE post_id=p.id) as like_count,
               (SELECT COUNT(*) FROM likes WHERE post_id=p.id AND user_id=?) as user_liked,
               (SELECT COUNT(*) FROM comments WHERE post_id=p.id) as comment_count
        FROM posts p JOIN users u ON p.user_id=u.id
        WHERE p.user_id=? AND p.post_type='post' ORDER BY p.created_at DESC
    ''', (session['user_id'], profile_user['id'])).fetchall()
    friend_status = None
    if profile_user['id'] != session['user_id']:
        rel = db.execute('''SELECT * FROM friendships
            WHERE (requester_id=? AND addressee_id=?) OR (requester_id=? AND addressee_id=?)''',
            (session['user_id'], profile_user['id'], profile_user['id'], session['user_id'])).fetchone()
        if rel:
            friend_status = 'friends' if rel['status']=='accepted' else ('pending_sent' if rel['requester_id']==session['user_id'] else 'pending_received')
    friends_count = db.execute('SELECT COUNT(*) as c FROM friendships WHERE (requester_id=? OR addressee_id=?) AND status="accepted"',
                                (profile_user['id'],)*2).fetchone()['c']
    notif_count = db.execute('SELECT COUNT(*) as cnt FROM notifications WHERE user_id=? AND is_read=0', (session['user_id'],)).fetchone()['cnt']
    db.close()
    return render_template('profile.html', profile_user=profile_user, posts=posts,
                           friend_status=friend_status, current=current,
                           friends_count=friends_count, notif_count=notif_count, owner=is_owner())

@app.route('/profile/edit', methods=['GET','POST'])
@login_required
def edit_profile():
    db = get_db(); user = get_current_user()
    if request.method == 'POST':
        bio    = request.form.get('bio','').strip()
        avatar = save_file(request.files.get('avatar'), ALLOWED_IMG) or user['avatar']
        cover  = save_file(request.files.get('cover'),  ALLOWED_IMG) or user['cover']
        db.execute('UPDATE users SET bio=?,avatar=?,cover=? WHERE id=?',
                   (bio, avatar, cover, session['user_id']))
        db.commit(); db.close()
        return redirect(url_for('profile', username=session['username']))
    db.close()
    return render_template('edit_profile.html', user=user)

# ── Admin ─────────────────────────────────────────────────
@app.route('/admin')
@owner_required
def admin():
    db    = get_db()
    users = db.execute('SELECT * FROM users ORDER BY created_at DESC').fetchall()
    posts = db.execute('SELECT p.*,u.username,u.role FROM posts p JOIN users u ON p.user_id=u.id ORDER BY p.created_at DESC LIMIT 100').fetchall()
    stats = {
        'users':    db.execute('SELECT COUNT(*) as c FROM users').fetchone()['c'],
        'posts':    db.execute('SELECT COUNT(*) as c FROM posts').fetchone()['c'],
        'messages': db.execute('SELECT COUNT(*) as c FROM messages').fetchone()['c'],
        'banned':   db.execute('SELECT COUNT(*) as c FROM users WHERE is_banned=1').fetchone()['c'],
        'reels':    db.execute('SELECT COUNT(*) as c FROM reels').fetchone()['c'],
        'groups':   db.execute('SELECT COUNT(*) as c FROM groups').fetchone()['c'],
    }
    db.close()
    return render_template('admin.html', users=users, posts=posts, stats=stats)

@app.route('/admin/ban/<int:uid>', methods=['POST'])
@owner_required
def ban_user(uid):
    db = get_db(); t = db.execute('SELECT * FROM users WHERE id=?', (uid,)).fetchone()
    if t and t['role']!='owner': db.execute('UPDATE users SET is_banned=1 WHERE id=?',(uid,)); db.commit(); flash(f"Banned '{t['username']}'.","success")
    else: flash("Cannot ban owner.","error")
    db.close(); return redirect(url_for('admin'))

@app.route('/admin/unban/<int:uid>', methods=['POST'])
@owner_required
def unban_user(uid):
    db = get_db(); t = db.execute('SELECT username FROM users WHERE id=?',(uid,)).fetchone()
    db.execute('UPDATE users SET is_banned=0 WHERE id=?',(uid,)); db.commit()
    flash(f"Unbanned '{t['username']}'.","success"); db.close(); return redirect(url_for('admin'))

@app.route('/admin/delete_user/<int:uid>', methods=['POST'])
@owner_required
def delete_user(uid):
    db = get_db(); t = db.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone()
    if t and t['role']!='owner':
        for q in ['DELETE FROM posts WHERE user_id=?','DELETE FROM comments WHERE user_id=?',
                  'DELETE FROM likes WHERE user_id=?','DELETE FROM reels WHERE user_id=?',
                  'DELETE FROM stories WHERE user_id=?','DELETE FROM notifications WHERE user_id=?']:
            db.execute(q,(uid,))
        db.execute('DELETE FROM messages WHERE sender_id=? OR receiver_id=?',(uid,uid))
        db.execute('DELETE FROM friendships WHERE requester_id=? OR addressee_id=?',(uid,uid))
        db.execute('DELETE FROM users WHERE id=?',(uid,))
        db.commit(); flash(f"Deleted '{t['username']}'.","success")
    else: flash("Cannot delete owner.","error")
    db.close(); return redirect(url_for('admin'))

@app.route('/admin/delete_post/<int:pid>', methods=['POST'])
@owner_required
def admin_delete_post(pid):
    db = get_db()
    db.execute('DELETE FROM comments WHERE post_id=?',(pid,))
    db.execute('DELETE FROM likes WHERE post_id=?',(pid,))
    db.execute('DELETE FROM posts WHERE id=?',(pid,))
    db.commit(); db.close(); flash('Post deleted.','success')
    return redirect(url_for('admin'))

@app.route('/admin/set_role/<int:uid>', methods=['POST'])
@owner_required
def set_role(uid):
    role = request.form.get('role','user'); db = get_db()
    t = db.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone()
    if t and t['role']!='owner': db.execute('UPDATE users SET role=? WHERE id=?',(role,uid)); db.commit(); flash(f"Role updated.","success")
    else: flash("Cannot change owner.","error")
    db.close(); return redirect(url_for('admin'))

# ── Friends ───────────────────────────────────────────────
@app.route('/friend/request/<int:uid>', methods=['POST'])
@login_required
def friend_request(uid):
    db = get_db()
    try:
        db.execute('INSERT INTO friendships (requester_id,addressee_id) VALUES (?,?)',(session['user_id'],uid)); db.commit()
        target = db.execute('SELECT username FROM users WHERE id=?',(uid,)).fetchone()
        add_notification(uid, session['user_id'], 'friend_request', f'{session["username"]} sent you a friend request', f'/profile/{session["username"]}')
    except: pass
    db.close(); return redirect(request.referrer or url_for('feed'))

@app.route('/friend/accept/<int:uid>', methods=['POST'])
@login_required
def friend_accept(uid):
    db = get_db()
    db.execute("UPDATE friendships SET status='accepted' WHERE requester_id=? AND addressee_id=?",(uid,session['user_id']))
    db.commit(); db.close(); return redirect(request.referrer or url_for('friends'))

@app.route('/friend/decline/<int:uid>', methods=['POST'])
@login_required
def friend_decline(uid):
    db = get_db(); db.execute('DELETE FROM friendships WHERE requester_id=? AND addressee_id=?',(uid,session['user_id']))
    db.commit(); db.close(); return redirect(request.referrer or url_for('friends'))

@app.route('/friend/remove/<int:uid>', methods=['POST'])
@login_required
def friend_remove(uid):
    db = get_db()
    db.execute('DELETE FROM friendships WHERE (requester_id=? AND addressee_id=?) OR (requester_id=? AND addressee_id=?)',
               (session['user_id'],uid,uid,session['user_id']))
    db.commit(); db.close(); return redirect(request.referrer or url_for('friends'))

@app.route('/friends')
@login_required
def friends():
    db = get_db(); user = get_current_user()
    accepted         = db.execute('SELECT u.* FROM users u JOIN friendships f ON (f.requester_id=u.id OR f.addressee_id=u.id) WHERE (f.requester_id=? OR f.addressee_id=?) AND f.status="accepted" AND u.id!=?',(session['user_id'],)*3).fetchall()
    pending_received = db.execute('SELECT u.* FROM users u JOIN friendships f ON f.requester_id=u.id WHERE f.addressee_id=? AND f.status="pending"',(session['user_id'],)).fetchall()
    pending_sent     = db.execute('SELECT u.* FROM users u JOIN friendships f ON f.addressee_id=u.id WHERE f.requester_id=? AND f.status="pending"',(session['user_id'],)).fetchall()
    notif_count      = db.execute('SELECT COUNT(*) as cnt FROM notifications WHERE user_id=? AND is_read=0',(session['user_id'],)).fetchone()['cnt']
    db.close()
    return render_template('friends.html', accepted=accepted, pending_received=pending_received,
                           pending_sent=pending_sent, user=user, notif_count=notif_count)

# ── Search ────────────────────────────────────────────────
@app.route('/search')
@login_required
def search():
    q = request.args.get('q','').strip(); db = get_db(); user = get_current_user(); users=[]
    if q:
        users = db.execute('''
            SELECT u.*,
                   (SELECT status FROM friendships WHERE (requester_id=? AND addressee_id=u.id) OR (requester_id=u.id AND addressee_id=?)) as fstatus,
                   (SELECT requester_id FROM friendships WHERE (requester_id=? AND addressee_id=u.id) OR (requester_id=u.id AND addressee_id=?)) as f_requester
            FROM users u WHERE u.username LIKE ? AND u.id!=? LIMIT 20
        ''', (session['user_id'],)*4 + (f'%{q}%', session['user_id'])).fetchall()
    notif_count = db.execute('SELECT COUNT(*) as cnt FROM notifications WHERE user_id=? AND is_read=0',(session['user_id'],)).fetchone()['cnt']
    db.close()
    return render_template('search.html', users=users, q=q, user=user, notif_count=notif_count)

# ── Chat ──────────────────────────────────────────────────
@app.route('/chat')
@login_required
def chat_list():
    db = get_db(); user = get_current_user()
    friends_list = db.execute('''
        SELECT u.*,
               (SELECT content FROM messages WHERE (sender_id=u.id AND receiver_id=?) OR (sender_id=? AND receiver_id=u.id) ORDER BY created_at DESC LIMIT 1) as last_msg,
               (SELECT COUNT(*) FROM messages WHERE sender_id=u.id AND receiver_id=? AND is_read=0) as unread
        FROM users u JOIN friendships f ON (f.requester_id=u.id OR f.addressee_id=u.id)
        WHERE (f.requester_id=? OR f.addressee_id=?) AND f.status='accepted' AND u.id!=?
    ''', (session['user_id'],)*6).fetchall()
    notif_count = db.execute('SELECT COUNT(*) as cnt FROM notifications WHERE user_id=? AND is_read=0',(session['user_id'],)).fetchone()['cnt']
    db.close()
    return render_template('chat_list.html', friends=friends_list, user=user, notif_count=notif_count)

@app.route('/chat/<int:friend_id>')
@login_required
def chat(friend_id):
    db = get_db(); user = get_current_user()
    friend   = db.execute('SELECT * FROM users WHERE id=?',(friend_id,)).fetchone()
    messages = db.execute('SELECT m.*,u.username,u.avatar FROM messages m JOIN users u ON m.sender_id=u.id WHERE (m.sender_id=? AND m.receiver_id=?) OR (m.sender_id=? AND m.receiver_id=?) ORDER BY m.created_at ASC',
                          (session['user_id'],friend_id,friend_id,session['user_id'])).fetchall()
    db.execute('UPDATE messages SET is_read=1 WHERE sender_id=? AND receiver_id=?',(friend_id,session['user_id']))
    db.commit(); db.close()
    return render_template('chat.html', friend=friend, messages=messages, user=user)

# ── SocketIO ──────────────────────────────────────────────
@socketio.on('join')
def on_join(data): join_room(data['room'])

@socketio.on('send_message')
def handle_message(data):
    sid = session.get('user_id'); rid = data['receiver_id']; content = data['content']
    if sid and content:
        db = get_db()
        db.execute('INSERT INTO messages (sender_id,receiver_id,content) VALUES (?,?,?)',(sid,rid,content))
        db.commit()
        sender = db.execute('SELECT * FROM users WHERE id=?',(sid,)).fetchone(); db.close()
        room = f"chat_{min(sid,rid)}_{max(sid,rid)}"
        emit('new_message',{'content':content,'sender_id':sid,'username':sender['username'],'avatar':sender['avatar'],'timestamp':datetime.datetime.now().strftime('%H:%M')},room=room)
        add_notification(rid, sid, 'message', f'{sender["username"]} sent you a message', f'/chat/{sid}')

@socketio.on('join_chat')
def on_join_chat(data):
    uid1=session.get('user_id'); uid2=data['friend_id']
    join_room(f"chat_{min(uid1,uid2)}_{max(uid1,uid2)}")

@socketio.on('register_user')
def register_user(data): join_room(f"user_{session['user_id']}")

@socketio.on('call_user')
def call_user(data):
    emit('incoming_call',{'caller_id':session['user_id'],'caller_name':session['username'],'call_type':data.get('call_type','video'),'room':data['room']},room=f"user_{data['target_id']}")

@socketio.on('call_response')
def call_response(data):
    emit('call_response',{'accepted':data['accepted'],'room':data['room']},room=f"user_{data['target_id']}")

@socketio.on('live_chat')
def live_chat(data):
    emit('live_message',{'username':session['username'],'content':data['content'],'ts':datetime.datetime.now().strftime('%H:%M')},room=f"live_{data['room']}")

@socketio.on('join_live')
def join_live(data):
    join_room(f"live_{data['room']}")
    db = get_db(); db.execute('UPDATE livestreams SET viewers=viewers+1 WHERE room=?',(data['room'],)); db.commit(); db.close()
    emit('viewer_count',{'count': get_db().execute('SELECT viewers FROM livestreams WHERE room=?',(data['room'],)).fetchone()['viewers']},room=f"live_{data['room']}")

@socketio.on('webrtc_offer')
def webrtc_offer(data): emit('webrtc_offer',data['offer'],room=data['room'],include_self=False)

@socketio.on('webrtc_answer')
def webrtc_answer(data): emit('webrtc_answer',data['answer'],room=data['room'],include_self=False)

@socketio.on('webrtc_ice')
def webrtc_ice(data): emit('webrtc_ice',data['candidate'],room=data['room'],include_self=False)

@app.route('/call/<room>')
@login_required
def call_page(room): return render_template('call.html', room=room, user=get_current_user())

if __name__ == '__main__':
    init_db()
    port = int(os.environ.get('PORT', 5000))
    socketio.run(app, debug=False, host='0.0.0.0', port=port)
