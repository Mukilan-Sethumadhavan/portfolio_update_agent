import json
import time
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
from config import YOUTUBE_API_KEY

def get_transcript(video_id):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        return " ".join([x['text'] for x in transcript])
    except (TranscriptsDisabled, NoTranscriptFound):
        return None
    except Exception as e:
        return None

def get_channel_subscribers(youtube, channel_id):
    try:
        response = youtube.channels().list(
            part="statistics",
            id=channel_id
        ).execute()
        if response["items"]:
            subs = int(response["items"][0]["statistics"].get("subscriberCount", 0))
            return subs
    except:
        pass
    return 0

def search_top_youtube_videos(query, max_results=50):
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    request = youtube.search().list(
        q=query,
        part="snippet",
        type="video",
        order="relevance",
        maxResults=min(max_results, 50)
    )
    response = request.execute()

    results = []

    for item in response["items"]:
        video_id = item["id"]["videoId"]
        title = item["snippet"]["title"]
        channel_title = item["snippet"]["channelTitle"]
        channel_id = item["snippet"]["channelId"]
        published_at = item["snippet"]["publishedAt"]
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        transcript = get_transcript(video_id)
        if transcript is None:
            continue

        subs = get_channel_subscribers(youtube, channel_id)
        if subs < 100_000:
            continue

        results.append({
            "title": title,
            "channel": channel_title,
            "published": published_at,
            "url": video_url,
            "transcript": transcript
        })

        time.sleep(0.5)  # Respect API rate limits

    return results

def get_company_youtube_data(company_name: str, max_results=50):
    """
    Main function to get YouTube data for a company
    Returns standardized data format
    """
    print(f"Processing YouTube videos for: {company_name}")
    try:
        videos = search_top_youtube_videos(company_name, max_results=max_results)
        print(f"Found {len(videos)} YouTube videos for {company_name}")
        return videos
    except Exception as e:
        print(f"Error processing YouTube videos for {company_name}: {e}")
        return []


# ----------------------------
# Run and save to JSON
# ----------------------------
if __name__ == "__main__":
    company = input("Enter a company name (e.g., SpaceX): ")
    print(f"\n🔍 Searching top YouTube videos about '{company}'...\n")

    videos = search_top_youtube_videos(company, max_results=50)

    if not videos:
        print("⚠️ No valid videos found.")
    else:
        # Save to JSON
        filename = f"youtube_top_videos_{company.lower().replace(' ', '_')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(videos, f, ensure_ascii=False, indent=4)
        print(f"\n✅ Results saved to '{filename}' with {len(videos)} videos.")
