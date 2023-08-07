from beanie import Document

class crawlerinstance(Document):
    url: str
    limit: int | None
    userId: str
    same_domain: bool | None
    status: int

    class Config:
        schema_extra = {
            "example": {
                "url:": "Url for onion link to scrape",
                "limit": "Maximum links to scrape",
                "userId": "Owner of the crawler",
                "same_domain": "Whether you want to go to urls found linking to other domains",
                "status": "Current status of the Crawler Instance"
            }
        }

    class Settings:
        name="crawlerinstance"