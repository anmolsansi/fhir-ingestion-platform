from app.db.session import engine
from app.models.base import Base
from app.models import tables  # noqa: F401


def main():
    Base.metadata.create_all(bind=engine)
    print("DB tables created.")


if __name__ == "__main__":
    main()
