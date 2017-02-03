var computeEdges = function(nodes) {
  
  var index = {};
  var i = 0;
  nodes.forEach(function(n){
    index[n.uri] = i++;
  });
  
  var edges = [];
  nodes.forEach(function(s){
    var si = index[s.uri];
    nodes.forEach(function(t){
      if (s.uri != t.uri) {
        edges.push({
          source: si,
          target: index[t.uri],
          rel: classifyRel(s, t)
        })
      }
    });
  })
  
  return edges;
}

var classifyRel = function(s, t) {
  if (s.direct.indexOf(t.uri) > -1) {
    return "direct";
  }
  if (s.explicit.indexOf(t.uri) > -1) {
    return "explicit";
  }
  return "indirect";
}

var limit = function(x, lim) {
  return Math.max(Math.min(x, lim), 0);
}


var color = d3.scale.category20();
var graph = d3.select("body").append("svg").attr("class", "fullscreen");

var renderGraph = function(error, data) {

  var width = window.innerWidth;
  var height= window.innerHeight;
  
  var nodes = data.content;
  var edges = computeEdges(nodes);
  
  nodes.forEach(function(n){
    if (n.fixed) {
      n.x = width/2;
      n.y = height/2;
    }
  });
  
  var force = d3.layout.force()
    .charge(-150)
    .linkDistance(function(l) { return l.rel == "indirect" ? 450 : 200; })
    .linkStrength(function(l) { return l.rel == "indirect" ? 0.8 : 0.2; })
    .size([ width, height])
    .nodes(nodes)
    .links(edges)
    .start();
  
  var link = graph.selectAll("line")
      .data(edges)
    .enter()
      .append("line")
        .attr("class", function(d) {return d.rel;});
  
  var radius = 8;
  var textDx = 12;
  
  var node = graph.selectAll(".node")
      .data(nodes)
    .enter()
      .append("g")
      .attr("class", "node")
      .call(force.drag)
      .each(function() { //create node label
        var nodeGroup = d3.select(this);
        
        var text = nodeGroup
          .append("text")
            .attr("dx", textDx)
            .attr("dy", 4)
            .text(function(d) { return d.uri });
        
        nodeGroup
          .insert("rect", ":first-child")
            .attr("width", text.node().getBBox().width + textDx + radius*2)
            .attr("height", radius * 2)
            .attr("transform", "translate("+radius+",-"+radius+")")
            .attr("rx", radius)
            .attr("ry", radius)
            .style("stroke", function(d) { return color(d.source); });
        
        nodeGroup
          .append("circle")
            .attr("r", radius)
            .style("fill", function(d) { return color(d.source); });
        
      });
  
  //update node and line positions on tick
  force.on("tick", function() {
    link
      .attr("x1", function(d) { return limit(d.source.x, width); })
      .attr("y1", function(d) { return limit(d.source.y, height); })
      .attr("x2", function(d) { return limit(d.target.x, width); })
      .attr("y2", function(d) { return limit(d.target.y, height); });
    
    node.attr("transform", function(d) {
      return "translate(" + limit(d.x,width) + "," + limit(d.y,height) + ")";
    })
    .each(function(d, i) {
      //move label to left if node in left of image
      var leftSide = d.x < width / 2;
      
      var text = d3.select(this).select("text")
        .style("text-anchor", function(d) { return leftSide ? "end" : "start"; })
        .attr("dx", function(d) { return textDx * (leftSide ? -1 : 1); });
      
      d3.select(this).select("rect")
      .attr("transform", function(d) {
        x = -radius - (leftSide ? text.node().getBBox().width + textDx : 0);
        return "translate(" + x + ",-"+radius+")";
      })
    })
    
  });

}

if (id) {
  d3.json("graph/data.json?id=" + id + "&min_edges=" + min_edges, renderGraph);
} else if (uri) {
  d3.json("graph/data.json?uri=" + uri + "&min_edges=" + min_edges, renderGraph);
}