// /* Goal: convert the relative-to-docroot urls that we upload into the Algolia
//    index into relative-to-the-current-page urls that a user can click.  Using
//    relative urls means that even on-prem clusters can use Algolia search without
//    being redirected to a different domain. */

// // Find the path to the root index.html, from the special rel=root link.
// // Example indexpath:  ../../index.html
// const relroot = document.querySelectorAll("[rel=root]")[0];
// // Extract the href as a string literal, to avoid url normalization.
// const indexpath = relroot.attributes["href"].value;

// // Our docroot is the directory containing index.html.
// const docroot = indexpath.split("/").slice(0, -1).join("/");

// // Extract the version, for picking the correct Algolia index.
// let version = relroot.attributes["version"].value;
// if (version.includes("-dev")) {
//   /* Dev builds search against a special dev index that is update with every
//      push to master. */
//   version = "dev";
// }else if (version.includes("-rc")) {
//   /* Each release candidate publishes against the actual version without the
//      "-rc" in the name. */
//   version = version.slice(0, version.indexOf("-rc"));
// }
// try {
docsearch({
    container: "#search-algolia",
    appId: "NTAFG04CW0",
    indexName: "getdaft",
    inputSelector: '#search-input',
    apiKey: "92105950b38d863a5f2f419bc96d9f75",
});
document.getElementById("search-fallback").style.display = "none";

// transformItems(items) {
//     return items.map((item) => {
//     /* The url we scrape for the algolia index is relative to the docroot,
//         but if docroot is the empty string then the item.url is already the
//         correct relative path. */
//     if (docroot !== "") {
//         item.url = docroot + "/" + item.url;
//     }
//     /* Convert from a relative URL to an absolute URL.  It is important we
//         emit absolute URLs rather than relative ones because the history
//         entries in the search bar modal caches the URLs we emit here.  If we
//         cache relative paths, then those history entries are wrong when
//         clicked on from other pages. */
//     item.url = new URL(item.url, window.location).href;
//     return item;
//     });
// },