import requests
import json
import time
from datetime import datetime
import os

def fetch_products(token, page_size=20, total_needed=500):
    """
    Fetch recent Product Hunt launches using GraphQL v2 API.
    """
    products = []
    url = "https://api.producthunt.com/v2/api/graphql"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # FIXED QUERY: Added proper nesting for topics and removed hardcoded 'first'
    query = """
    query GetRecentPosts($cursor: String, $first: Int) {
      posts(first: $first, after: $cursor, order: NEWEST) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            name
            tagline
            votesCount
            url
            topics {
              edges {
                node {
                  name
                }
              }
            }
            createdAt
          }
        }
      }
    }
    """

    cursor = None
    while len(products) < total_needed:
        # Variables must match the names in the query definition ($cursor, $first)
        variables = {"first": page_size, "cursor": cursor}

        for attempt in range(3):
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    json={"query": query, "variables": variables}
                )
                
                # Catch HTTP errors (like 401 or 403)
                response.raise_for_status()
                result = response.json()

                # Catch GraphQL internal errors
                if "errors" in result:
                    print(f"GraphQL Errors: {result['errors']}")
                    raise Exception("GraphQL query failed")

                data = result.get("data", {}).get("posts", {})
                edges = data.get("edges", [])
                page_info = data.get("pageInfo", {})

                if not edges:
                    print("No more posts available.")
                    return products

                for edge in edges:
                    node = edge["node"]
                    # FIXED: Extracting tags from the nested edges/node structure
                    tags = [topic["node"]["name"] for topic in node.get("topics", {}).get("edges", [])]
                    
                    product = {
                        "product_name": node.get("name", "N/A"),
                        "tagline": node.get("tagline", "N/A"),
                        "tags": tags,
                        "popularity": node.get("votesCount", 0),
                        "product_url": node.get("url", "N/A"), # v2 uses 'url' for the PH page
                        "created_at": node.get("createdAt"),
                        "scrape_timestamp": datetime.utcnow().isoformat()
                    }
                    products.append(product)

                print(f"Fetched {len(edges)} products, total so far: {len(products)}")

                if not page_info.get("hasNextPage") or len(products) >= total_needed:
                    break

                cursor = page_info.get("endCursor")
                time.sleep(1.5) # Safe rate limit
                break

            except Exception as e:
                print(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(5)
                else:
                    return products # Return what we have if all retries fail

    return products[:total_needed]

if __name__ == "__main__":
    # Use your generated Developer Token from the PH Dashboard
    token = "CCcQ1Tqyy0kgya04SSjsjNFmZi0begHVRvicoEdl_dA" 
    
    print("Starting scrape...")
    products = fetch_products(token, page_size=30, total_needed=500)

    if products:
        os.makedirs("data/raw", exist_ok=True)
        output_path = "data/raw/products_raw2.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(products, f, indent=4, ensure_ascii=False)

        print(f"\n✅ Success! Saved {len(products)} products to {output_path}")
    else:
        print("\n❌ Failed to collect data. Check your token and connection.")