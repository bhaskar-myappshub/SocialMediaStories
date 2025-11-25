import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username = Column(String(150), unique=True, nullable=False)
    followers = Column(JSONB, nullable=False, default=list)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    stories = relationship("Story", back_populates="user", cascade="all, delete-orphan")

class Story(Base):
    __tablename__ = "stories"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(PG_UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    s3_key = Column(String, nullable=False, unique=True)
    filename = Column(String, nullable=False)
    content_type = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    media_type = Column(String, nullable=False)  # 'image' or 'video'
    viewership = Column(String, default="public") # 'public' or 'followers'
    viewers = Column(JSONB, nullable=False, default=list)

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=False)

    user = relationship("User", back_populates="stories")
import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Integer, ForeignKey, Text, LargeBinary, UniqueConstraint, Boolean, func, Date, Enum
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB
from sqlalchemy.orm import relationship, declarative_base, backref

Base = declarative_base()


class Story(Base):
    __tablename__ = "stories"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Foreign key to users table
    user_id = Column(Integer, ForeignKey("users_.id", ondelete="CASCADE"), nullable=False)

    # File info
    s3_key = Column(String, nullable=False, unique=True)
    filename = Column(String, nullable=False)
    content_type = Column(String, nullable=False)
    size = Column(Integer, nullable=False)
    media_type = Column(String, nullable=False)  # 'image' or 'video'
    thumbnail_key = Column(String, nullable=False)

    # Viewers
    viewers = Column(JSONB, nullable=False, default=list)

    # Additional story metadata
    caption = Column(Text, nullable=True)
    duration_hours = Column(Integer, default=24)
    privacy = Column(String(50), default="public")

    # Complex nested objects stored as JSONB
    location = Column(JSONB, nullable=True)   # { name, latitude, longitude }
    mentions = Column(JSONB, default=list)    # [user_id1, user_id2, ...]
    hashtags = Column(JSONB, default=list)    # ["nature", "newyork"]
    music = Column(JSONB, nullable=True)      # { song_id, artist, start_time }
    stickers = Column(JSONB, default=list)    # [{ type, question, ... }]

    # Permissions
    allow_replies = Column(Boolean, default=True)
    allow_sharing = Column(Boolean, default=True)

    #archive, highlight, deleted_at details
    archive = Column(Boolean, default=False)
    highlight = Column(Boolean, default=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    # Time tracking
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime(timezone=True), nullable=False)

    # Relationships
    user = relationship("UserDB", back_populates="stories")
    comments = relationship("StoryComment", back_populates="story", cascade="all, delete-orphan", passive_deletes=True)
    reactions = relationship("StoryReaction", back_populates="story", cascade="all, delete-orphan", passive_deletes=True)
    highlights = relationship("Highlight", back_populates="story", cascade="all, delete-orphan")


class StoryReaction(Base):
    __tablename__ = "stories_reactions"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    story_id = Column(PG_UUID(as_uuid=True), ForeignKey("stories.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users_.id", ondelete="CASCADE"), nullable=False)
    reaction_type = Column(String, nullable=False)  # e.g. 'like', 'heart', 'smile', 'sad'
    reacted_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    story = relationship("Story", back_populates="reactions")
    user = relationship("UserDB")


class Comment(Base):
    __tablename__ = "comments"
 
    id = Column(Integer, primary_key=True, index=True)
    video_id = Column(PG_UUID(as_uuid=True), ForeignKey("videos.id"))
    user_id = Column(Integer, ForeignKey("users_.id"))
    parent_id = Column(Integer, ForeignKey("comments.id", ondelete="CASCADE"), nullable=True)
    text = Column(Text, nullable=True)
 
    gif_url = Column(String, nullable=True)
    gif_data = Column(LargeBinary, nullable=True)
 
    created_at = Column(DateTime, default=datetime.now(timezone.utc), nullable=False)
 
    # relationships
    user = relationship("UserDB", backref="comments", lazy="joined")
 
    # self-referential relationship for nested replies
    replies = relationship(
        "Comment",
        backref=backref("parent", remote_side=[id]),
        cascade="all, delete-orphan",
        passive_deletes=True
    )


class Reaction(Base):
    __tablename__ = "reactions"
 
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users_.id"), nullable=False)
    video_id = Column(PG_UUID(as_uuid=True), ForeignKey("videos.id", ondelete="CASCADE"), nullable=False)
    reaction_type = Column(String(50), nullable=False)  # e.g., "like", "heart", "smile", "sad"
    created_at = Column(DateTime, default=datetime.now(timezone.utc))
 
    __table_args__ = (UniqueConstraint("user_id", "video_id", "reaction_type", name="uix_reaction"),)


class Video(Base):
    __tablename__ = "videos"
 
    id = Column(PG_UUID(as_uuid=True), primary_key=True)
    user_id = Column(Integer, ForeignKey("users_.id"), nullable=False)
    s3_key = Column(String, nullable=False)  # raw upload key
    thumbnail_key = Column(String)  # thumbnail image
    hls_key = Column(String)  # HLS playlist key
    is_processed = Column(Boolean, default=False)
    title = Column(String(100))
    description = Column(Text)
    client_ip = Column(String(45))  # Store client IP address
    visibility = Column(String(20), default="public")
    created_at = Column(DateTime, default=datetime.now(timezone.utc))


class StoryComment(Base):
    __tablename__ = "story_comments"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    story_id = Column(PG_UUID(as_uuid=True), ForeignKey("stories.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users_.id", ondelete="CASCADE"), nullable=False)
    text = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

    # relationships
    user = relationship("UserDB", foreign_keys=[user_id], backref="story_comments", lazy="joined")
    story = relationship("Story", back_populates="comments", lazy="joined")


class CloseFriend(Base):
    __tablename__ = "close_friends"
    __table_args__ = (UniqueConstraint('user_id', 'close_friend_id', name='unique_close_friend'),)
 
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users_.id', ondelete='CASCADE'), nullable=False)
    close_friend_id = Column(Integer, ForeignKey('users_.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
 
    user = relationship("UserDB", foreign_keys=[user_id], back_populates="close_friends_given")
    close_friend_user = relationship("UserDB", foreign_keys=[close_friend_id], back_populates="close_friends_received")
 
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
 

class UserDB(Base):
    __tablename__ = "users_"
 
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    contact_info = Column(String(120), unique=True, nullable=False)
    bio = Column(Text, nullable=True)
    profile_image_key = Column(Text)
    cover_image_key = Column(Text)
    profile_visibility = Column(String(20), default="public")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    date_of_birth = Column(Date, nullable=True)
    display_name = Column(String(100), nullable=True)
    gender = Column(String(10), nullable=True)
    is_active = Column(Boolean, default=True)
    auto_archive_stories = Column(Boolean, default=False)

    stories = relationship("Story", back_populates="user", cascade="all, delete-orphan")


    def as_dict(self):
        data = {column.name: getattr(self, column.name) for column in self.__table__.columns}
        data["interests"] = [i.name for i in self.interests]
        return data

    close_friends_given = relationship(
        "CloseFriend",
        back_populates="user",
        foreign_keys=[CloseFriend.user_id],
        cascade="all, delete-orphan"
    )
    close_friends_received = relationship(
        "CloseFriend",
        back_populates="close_friend_user",
        foreign_keys=[CloseFriend.close_friend_id],
        cascade="all, delete-orphan"
    )

    account_history = relationship(
        "AccountHistory",
        back_populates="user",
        cascade="all, delete-orphan"
    )
 

class Follower(Base):
    __tablename__ = "followers"

    id = Column(Integer, primary_key=True)
    follower_id = Column(Integer, nullable=False)
    following_id = Column(Integer, nullable=False)
    status = Column(String(20), default="pending")  # pending, accepted, rejected
    created_at = Column(DateTime, default=func.now())
    blocked = Column(Boolean, default=False)

    __table_args__ = (
        UniqueConstraint('follower_id', 'following_id', name='uix_follower_following'),
    )


class Highlight(Base):
    __tablename__ = "highlights"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    story_id = Column(PG_UUID(as_uuid=True), ForeignKey("stories.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(100), nullable=False)  # highlight title (e.g., "Vacation", "Food")
    cover_image_key = Column(String, nullable=True)  # S3 key for cover image
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    archive = Column(Boolean, default=False)
    order = Column(Integer, nullable=False)

    # Relationships
    story = relationship("Story", back_populates="highlights")


class Sticker(Base):
    __tablename__ = "stickers"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    story_id = Column(PG_UUID(as_uuid=True), ForeignKey("stories.id", ondelete="SET NULL"), nullable=True)
    type = Column(String, nullable=False)
    question_text = Column(Text, nullable=True)   # e.g. "How’s your day?" or "Pick your favorite"
    emoji_icon = Column(String, nullable=True)    # e.g. 🔥 for slider reaction
    options = Column(Text, nullable=True)         # JSON string: ["Yes","No"] or ["A","B","C"]
    correct_option = Column(Integer, nullable=True)  # for quizzes
    position = Column(JSONB, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # relationships
    responses = relationship("StickerResponse", back_populates="sticker", cascade="all, delete-orphan")


class StickerResponse(Base):
    __tablename__ = "sticker_responses"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sticker_id = Column(PG_UUID(as_uuid=True), ForeignKey("stickers.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users_.id", ondelete="CASCADE"), nullable=False)

    # actual response content depends on sticker type
    selected_option = Column(Integer, nullable=True)  # index of selected poll/quiz option
    slider_value = Column(Integer, nullable=True)       # e.g. 0–100
    
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    # relationships
    sticker = relationship("Sticker", back_populates="responses")
    user = relationship("UserDB", backref="sticker_responses", lazy="joined")


class VideoView(Base):
    __tablename__ = "video_views"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    user_id = Column(
        Integer,
        ForeignKey("users_.id", ondelete="CASCADE"),
        nullable=False
    )

    video_id = Column(
        PG_UUID(as_uuid=True),
        ForeignKey("videos.id", ondelete="CASCADE"),
        nullable=False
    )

    viewed_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )

    # Relationships
    user = relationship("UserDB", backref="video_views", lazy="joined")
    video = relationship("Video", backref="views", lazy="joined")


class AccountHistory(Base):
    __tablename__ = "account_history"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    user_id = Column(Integer, ForeignKey("users_.id", ondelete="CASCADE"), nullable=False)

    # e.g., "login", "username_change", "email_change", "bio_update", "password_reset"
    event_type = Column(String(100), nullable=False)

    # JSON metadata with old/new values or extra info about the event
    data = Column(JSONB, nullable=False)

    ip_address = Column(String(50), nullable=True)

    device = Column(String(255), nullable=True)

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Relationship to user
    user = relationship("UserDB", back_populates="account_history")