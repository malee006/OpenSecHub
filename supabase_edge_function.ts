import { serve } from "https://deno.land/std@0.178.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.42.0";
// --- Environment Variables ---
const GITHUB_TOKEN = Deno.env.get("GITHUB_TOKEN");
const BROWSERLESS_TOKEN = Deno.env.get("BROWSERLESS_TOKEN"); // Kept for completeness, but screenshotting is disabled
const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
// --- Constants ---
const TOPICS = [
  "security-tools",
  "security",
  "cybersecurity",
  "pentesting",
  "penetration-testing",
  "hacking",
  "python",
  "osint",
  "vulnerability-scanner",
  "threat-intelligence",
  "security-scanner",
  "bugbounty",
  "infosec",
  "security-audit",
  "malware-analysis",
  "hacktoberfest",
  "vulnerability-scanners",
  "pentest",
  "golang",
  "scanner",
  "aws-security",
  "kubernetes",
  "devsecops",
  "dynamic-analysis",
  "offensive-security",
  "web-security",
  "ios-security",
  "android-security",
  "mobile-security",
  "network-security",
  "ctf",
  "appsec",
  "application-security",
  "kubernetes-security",
  "api-security",
  "grc",
  "nist",
  "cloud-security",
  "saas-security",
  "cspm",
  "security-audit"
];
const SECURITY_TOOLS_SHARDS = [
  "stars:>1000",
  "stars:501..1000",
  "stars:101..500",
  "stars:0..100"
];
const GITHUB_GRAPHQL_QUERY = `
  query ($query: String!, $cursor: String) {
    search(query: $query, type: REPOSITORY, first: 50, after: $cursor) {
      pageInfo {
        endCursor
        hasNextPage
      }
      nodes {
        ... on Repository {
          name
          owner { login }
          url
          description
          stargazerCount
          forkCount
          issues { totalCount }
          watchers { totalCount }
          primaryLanguage { name }
          licenseInfo { spdxId }
          repositoryTopics(first: 10) { nodes { topic { name } } }
          visibility
          defaultBranchRef { name }
          createdAt
          pushedAt
          updatedAt
          object(expression: "HEAD:README.md") { ... on Blob { text } }
          openGraphImageUrl
        }
      }
    }
    rateLimit {
      remaining
      resetAt
    }
  }
`;
const JOB_STATE_ID = "github_scraper_v1"; // Identifier for our job's state in Supabase
// --- Helper Functions ---
/**
 * Fetches data from GitHub GraphQL API with rate limit handling and retries.
 */ async function graphqlFetch(query, variables) {
  const GITHUB_API_URL = "https://api.github.com/graphql";
  let retries = 0;
  const maxRetries = 3;
  let delay = 1000; // Start with 1 second delay
  while(true){
    if (!GITHUB_TOKEN) {
      console.error("GITHUB_TOKEN is not configured in graphqlFetch.");
      throw new Error("Critical: GITHUB_TOKEN is not available.");
    }
    let response;
    try {
      console.log(`Making GitHub API request (Attempt ${retries + 1}/${maxRetries + 1}) for query: ${variables.query}, cursor: ${variables.cursor || 'start'}`);
      response = await fetch(GITHUB_API_URL, {
        method: "POST",
        headers: {
          Authorization: `bearer ${GITHUB_TOKEN}`,
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Accept-Encoding": "identity",
          "User-Agent": "Supabase-Edge-Function-GitHub-Scraper"
        },
        body: JSON.stringify({
          query,
          variables
        })
      });
      if (!response.ok) {
        const rateLimitRemaining = response.headers.get("x-ratelimit-remaining");
        const rateLimitReset = response.headers.get("x-ratelimit-reset");
        if (response.status === 401) {
          console.error("GitHub API Error: 401 Unauthorized. Check GITHUB_TOKEN validity and permissions.");
          throw new Error("GitHub API Unauthorized. Invalid GITHUB_TOKEN or insufficient permissions.");
        }
        if (response.status === 403 || response.status === 429) {
          console.warn(`Rate limit hit. Status: ${response.status}. Remaining: ${rateLimitRemaining}, Reset: ${rateLimitReset}`);
          if (rateLimitReset) {
            const resetTime = parseInt(rateLimitReset) * 1000;
            const currentTime = Date.now();
            const sleepDuration = Math.max(0, resetTime - currentTime + 5000); // Wait 5s past reset
            console.warn(`Sleeping for ${sleepDuration / 1000} seconds until rate limit reset.`);
            await new Promise((resolve)=>setTimeout(resolve, sleepDuration));
            continue; // Retry the request
          }
        }
        if ((response.status === 502 || response.status === 504) && retries < maxRetries) {
          retries++;
          console.warn(`Received ${response.status} ${response.statusText}. Retrying in ${delay / 1000}s (Attempt ${retries}/${maxRetries}).`);
          await new Promise((resolve)=>setTimeout(resolve, delay));
          delay *= 2; // Exponential backoff
          continue;
        }
        let errorText = `${response.status} ${response.statusText}`;
        try {
          errorText = await response.text();
        } catch (e) {}
        throw new Error(`GitHub API HTTP Error: ${response.status}. Response: ${errorText.substring(0, 500)}`);
      }
      let responseText;
      try {
        responseText = await response.text();
      } catch (e) {
        throw new Error(`Failed to read GitHub API response body: ${e.message}`);
      }
      let data;
      try {
        data = JSON.parse(responseText);
      } catch (jsonError) {
        console.error("Raw GitHub API response (failed to parse JSON):", responseText.substring(0, 1000));
        throw new Error(`GitHub API JSON parsing error: ${jsonError.message}`);
      }
      if (data.errors) {
        console.error("GitHub GraphQL Errors:", JSON.stringify(data.errors, null, 2));
        // Check for specific errors that might indicate a bad cursor or query
        if (data.errors.some((e)=>e.message?.includes("cursor") || e.message?.includes("Variable '$cursor'"))) {
          throw new Error(`GitHub GraphQL API error related to cursor: ${JSON.stringify(data.errors)}`);
        }
        throw new Error("GitHub GraphQL API returned errors.");
      }
      const rateLimit = data?.data?.rateLimit;
      if (rateLimit && rateLimit.remaining < 50) {
        const resetTime = new Date(rateLimit.resetAt).getTime();
        const currentTime = Date.now();
        const sleepDuration = Math.max(0, resetTime - currentTime + 10000); // Wait 10s past reset
        console.warn(`GraphQL rate limit approaching (${rateLimit.remaining} remaining). Sleeping for ${sleepDuration / 1000}s.`);
        await new Promise((resolve)=>setTimeout(resolve, sleepDuration));
      // No continue here, let the current request complete, sleep applies before next potential request
      }
      return data.data;
    } catch (error) {
      console.error(`Error during graphqlFetch: ${error.message}`);
      if (retries < maxRetries && !(error.message.includes("Unauthorized") || error.message.includes("cursor"))) {
        retries++;
        console.warn(`Retrying graphqlFetch in ${delay / 1000}s (Attempt ${retries}/${maxRetries}).`);
        await new Promise((resolve)=>setTimeout(resolve, delay));
        delay *= 2;
        continue; // Go to the next iteration of the while loop to retry
      } else {
        console.error("Max retries reached or critical error in graphqlFetch. Throwing error.");
        throw error; // Re-throw the error if max retries are reached or it's a non-retryable error
      }
    }
  }
}
/**
 * Chunks an array into smaller arrays.
 */ function chunksOf(arr, n) {
  const result = [];
  for(let i = 0; i < arr.length; i += n){
    result.push(arr.slice(i, i + n));
  }
  return result;
}
// --- Main Handler ---
serve(async (req)=>{
  const startTime = Date.now();
  // Increased max runtime to allow more processing, ensure Supabase function timeout is higher.
  // This is an internal target; the function might be killed by Supabase earlier.
  const MAX_RUNTIME_MS = 240000; // 4 minutes internal target, ensure Supabase timeout is ~5 mins
  let insertedCount = 0;
  let skippedCount = 0;
  let supabase;
  try {
    // Validate environment variables
    if (!GITHUB_TOKEN) throw new Error("GITHUB_TOKEN environment variable is not set.");
    if (!SUPABASE_URL) throw new Error("SUPABASE_URL environment variable is not set.");
    if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY environment variable is not set.");
    supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      global: {
        headers: {
          Authorization: req.headers.get("Authorization")
        }
      }
    });
    // Test GitHub API connectivity (optional, but good for immediate feedback)
    try {
      console.log("Testing GitHub API connectivity...");
      const testResult = await graphqlFetch(`query { viewer { login } }`, {});
      console.log("GitHub API test successful. Authenticated as:", testResult.viewer?.login);
    } catch (apiTestError) {
      console.error("GitHub API test failed:", apiTestError.message);
      return new Response(JSON.stringify({
        error: "Failed to connect to GitHub API",
        details: apiTestError.message
      }), {
        status: 503,
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    // 1. Fetch or Initialize Job State
    let { data: jobState, error: stateFetchError } = await supabase.from("job_state").select("*").eq("id", JOB_STATE_ID).single();
    if (stateFetchError && stateFetchError.code !== 'PGRST116') {
      console.error("Error fetching job state:", stateFetchError);
      throw new Error(`Failed to fetch job state: ${stateFetchError.message}`);
    }
    if (!jobState) {
      console.log("No existing job state found, initializing...");
      const { data: newState, error: initStateError } = await supabase.from("job_state").insert([
        {
          id: JOB_STATE_ID,
          current_topic_index: 0,
          current_security_tools_shard_index: 0,
          current_cursor: null,
          last_run_completed_cycle: false,
          updated_at: new Date().toISOString()
        }
      ]).select().single();
      if (initStateError) throw new Error(`Failed to initialize job state: ${initStateError.message}`);
      jobState = newState;
      console.log("Job state initialized:", jobState);
    } else {
      console.log("Fetched job state:", jobState);
    }
    // 2. Check for cycle completion and reset if needed
    if (jobState.last_run_completed_cycle) {
      console.log("Previous run completed a full cycle. Resetting to start a new cycle.");
      jobState.current_topic_index = 0;
      jobState.current_security_tools_shard_index = 0;
      jobState.current_cursor = null;
      jobState.last_run_completed_cycle = false;
      jobState.updated_at = new Date().toISOString();
      const { error: resetError } = await supabase.from("job_state").update(jobState).eq("id", JOB_STATE_ID);
      if (resetError) throw new Error(`Failed to reset job cycle state: ${resetError.message}`);
    }
    let currentTopicIndex = jobState.current_topic_index;
    let currentSecurityToolsShardIndex = jobState.current_security_tools_shard_index;
    let currentCursor = jobState.current_cursor;
    // 3. Check if all topics are already processed for the current cycle
    if (currentTopicIndex >= TOPICS.length) {
      console.log("All topics processed for this cycle. Marking as complete.");
      const { error: finalUpdateError } = await supabase.from("job_state").update({
        last_run_completed_cycle: true,
        current_topic_index: currentTopicIndex,
        updated_at: new Date().toISOString()
      }).eq("id", JOB_STATE_ID);
      if (finalUpdateError) console.error("Error marking cycle as complete at start:", finalUpdateError.message);
      return new Response(JSON.stringify({
        message: "All topics processed. Cycle complete.",
        durationMs: Date.now() - startTime
      }), {
        status: 200,
        headers: {
          "Content-Type": "application/json"
        }
      });
    }
    const topic = TOPICS[currentTopicIndex];
    let searchQuery;
    let processingUnitDescription;
    if (topic === "security-tools") {
      if (currentSecurityToolsShardIndex >= SECURITY_TOOLS_SHARDS.length) {
        console.log(`All shards for 'security-tools' processed. Moving to next topic.`);
        currentTopicIndex++;
        currentSecurityToolsShardIndex = 0;
        currentCursor = null; // Reset cursor for new topic/segment
        // Update state and prepare for next run or immediate continuation if logic allows
        const { error: updateError } = await supabase.from("job_state").update({
          current_topic_index: currentTopicIndex,
          current_security_tools_shard_index: currentSecurityToolsShardIndex,
          current_cursor: currentCursor,
          last_run_completed_cycle: currentTopicIndex >= TOPICS.length,
          updated_at: new Date().toISOString()
        }).eq("id", JOB_STATE_ID);
        if (updateError) console.error("Error updating state after security-tools shards:", updateError.message);
        if (currentTopicIndex >= TOPICS.length) {
          return new Response(JSON.stringify({
            message: "All topics completed after finishing security-tools shards.",
            durationMs: Date.now() - startTime
          }), {
            status: 200,
            headers: {
              "Content-Type": "application/json"
            }
          });
        }
        // If not all topics done, this run ends, next run picks up new topic.
        return new Response(JSON.stringify({
          message: `Completed shards for 'security-tools'. Next run for topic index ${currentTopicIndex}.`,
          durationMs: Date.now() - startTime
        }), {
          status: 200,
          headers: {
            "Content-Type": "application/json"
          }
        });
      }
      const shard = SECURITY_TOOLS_SHARDS[currentSecurityToolsShardIndex];
      searchQuery = `topic:${topic} ${shard} is:public is:not-fork archived:false`;
      processingUnitDescription = `topic: ${topic}, shard: ${shard} (#${currentSecurityToolsShardIndex})`;
    } else {
      searchQuery = `topic:${topic} is:public is:not-fork archived:false`;
      processingUnitDescription = `topic: ${topic} (#${currentTopicIndex})`;
    }
    console.log(`Processing ${processingUnitDescription}. Cursor: ${currentCursor || 'start'}`);
    console.log(`Memory usage: ${JSON.stringify(Deno.memoryUsage())}`);
    let hasNextPage = true;
    let pageNum = 0;
    while(hasNextPage){
      if (Date.now() - startTime > MAX_RUNTIME_MS - 15000) {
        console.warn("Approaching timeout limit. Saving current state and stopping for this run.");
        const { error: saveStateError } = await supabase.from("job_state").update({
          current_topic_index: currentTopicIndex,
          current_security_tools_shard_index: currentSecurityToolsShardIndex,
          current_cursor: currentCursor,
          last_run_completed_cycle: false,
          updated_at: new Date().toISOString()
        }).eq("id", JOB_STATE_ID);
        if (saveStateError) console.error("Error saving state before timeout:", saveStateError.message);
        return new Response(JSON.stringify({
          message: "Run timed out, progress saved.",
          inserted: insertedCount,
          skipped: skippedCount,
          durationMs: Date.now() - startTime,
          nextCursor: currentCursor
        }), {
          status: 200,
          headers: {
            "Content-Type": "application/json"
          }
        });
      }
      pageNum++;
      console.log(`  Fetching page ${pageNum} for ${processingUnitDescription}`);
      const variables = {
        query: searchQuery,
        cursor: currentCursor
      };
      let ghData;
      try {
        ghData = await graphqlFetch(GITHUB_GRAPHQL_QUERY, variables);
      } catch (error) {
        console.error(`Failed to fetch page ${pageNum} for ${processingUnitDescription}: ${error.message}. Saving state.`);
        // Save current progress before exiting due to this error
        const { error: saveStateError } = await supabase.from("job_state").update({
          current_topic_index: currentTopicIndex,
          current_security_tools_shard_index: currentSecurityToolsShardIndex,
          current_cursor: currentCursor,
          updated_at: new Date().toISOString()
        }).eq("id", JOB_STATE_ID);
        if (saveStateError) console.error("Error saving state on fetch error:", saveStateError.message);
        throw new Error(`GitHub fetch failed for ${processingUnitDescription}: ${error.message}`); // Propagate to main catch
      }
      const searchResult = ghData.search;
      const repos = searchResult.nodes;
      const rawToolRows = [];
      for (const repo of repos){
        if (!repo) continue;
        rawToolRows.push({
          full_name: `${repo.owner.login}/${repo.name}`,
          name: repo.name,
          owner: repo.owner.login,
          source_topic: topic,
          html_url: repo.url,
          description: repo.description,
          readme_md: repo.object?.text ? repo.object.text.substring(0, 64 * 1024) : null,
          preview_image_url: repo.openGraphImageUrl || null,
          stars: repo.stargazerCount,
          forks: repo.forkCount,
          issues: repo.issues.totalCount,
          watchers: repo.watchers.totalCount,
          language: repo.primaryLanguage?.name,
          license: repo.licenseInfo?.spdxId,
          topics: repo.repositoryTopics.nodes.map((n)=>n.topic.name),
          visibility: repo.visibility,
          default_branch: repo.defaultBranchRef?.name,
          created_at: repo.createdAt,
          pushed_at: repo.pushedAt,
          updated_at: repo.updatedAt
        });
      }
      if (rawToolRows.length > 0) {
        const rowChunks = chunksOf(rawToolRows, 20); // Reduced batch size for inserts
        for (const chunk of rowChunks){
          const { error: dbError, count: newlyInserted } = await supabase.from("RawTools").insert(chunk, {
            onConflict: "full_name",
            ignoreDuplicates: true
          });
          if (dbError) {
            console.error("Supabase insert error:", dbError.message);
          } else {
            insertedCount += newlyInserted || 0;
            skippedCount += chunk.length - (newlyInserted || 0);
          }
        }
        console.log(`  Processed ${rawToolRows.length} repos from page. Inserted: ${insertedCount}, Skipped: ${skippedCount} (cumulative for this run).`);
      }
      hasNextPage = searchResult.pageInfo.hasNextPage;
      currentCursor = searchResult.pageInfo.endCursor; // This is the cursor for the NEXT page
      if (!hasNextPage) {
        console.log(`  Finished all pages for ${processingUnitDescription}.`);
        currentCursor = null; // Reset cursor as this unit is done
        if (topic === "security-tools") {
          currentSecurityToolsShardIndex++;
          if (currentSecurityToolsShardIndex >= SECURITY_TOOLS_SHARDS.length) {
            currentTopicIndex++;
            currentSecurityToolsShardIndex = 0; // Reset for next 'security-tools' cycle (if any)
          }
        } else {
          currentTopicIndex++;
        }
        // Update state after completing a topic/shard
        const { error: updateStateError } = await supabase.from("job_state").update({
          current_topic_index: currentTopicIndex,
          current_security_tools_shard_index: currentSecurityToolsShardIndex,
          current_cursor: currentCursor,
          last_run_completed_cycle: currentTopicIndex >= TOPICS.length,
          updated_at: new Date().toISOString()
        }).eq("id", JOB_STATE_ID);
        if (updateStateError) console.error("Error updating state after topic/shard completion:", updateStateError.message);
        break; // Exit page loop for this topic/shard
      }
      await new Promise((resolve)=>setTimeout(resolve, 500)); // Delay between GitHub pages
    } // End while(hasNextPage)
    const durationMs = Date.now() - startTime;
    let finalMessage = `Processing of ${processingUnitDescription} segment finished.`;
    if (currentTopicIndex >= TOPICS.length) {
      finalMessage = "All topics and shards processed in this cycle.";
      // Ensure last_run_completed_cycle is set to true if not already by loop logic
      const { error: finalUpdateError } = await supabase.from("job_state").update({
        last_run_completed_cycle: true,
        current_topic_index: currentTopicIndex,
        updated_at: new Date().toISOString()
      }).eq("id", JOB_STATE_ID);
      if (finalUpdateError) console.error("Error finalizing cycle completion state:", finalUpdateError.message);
    }
    console.log(`Run completed. Duration: ${durationMs}ms. Inserted: ${insertedCount}, Skipped: ${skippedCount}`);
    return new Response(JSON.stringify({
      message: finalMessage,
      inserted: insertedCount,
      skipped: skippedCount,
      durationMs,
      currentTopicIndexProcessed: currentTopicIndex - 1,
      currentSecurityToolsShardIndexProcessed: currentSecurityToolsShardIndex - 1
    }), {
      status: 200,
      headers: {
        "Content-Type": "application/json"
      }
    });
  } catch (error) {
    console.error("Edge Function critical error:", error.stack || error.message);
    // Attempt to save current state if possible (might not have supabase client initialized if error is early)
    // The state saving within the loop or before timeout is more reliable for graceful exits.
    return new Response(JSON.stringify({
      error: error.message,
      inserted: insertedCount,
      skipped: skippedCount,
      durationMs: Date.now() - startTime
    }), {
      status: 500,
      headers: {
        "Content-Type": "application/json"
      }
    });
  }
});
