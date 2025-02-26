"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[5270],{95033:(t,e,i)=>{i.d(e,{CP:()=>c,HT:()=>u,PB:()=>h,aC:()=>o,lC:()=>n,m:()=>l,tk:()=>s});var r=i(85354),a=i(9010),s=(0,r.K2)((t,e)=>{let i=t.append("rect");if(i.attr("x",e.x),i.attr("y",e.y),i.attr("fill",e.fill),i.attr("stroke",e.stroke),i.attr("width",e.width),i.attr("height",e.height),e.name&&i.attr("name",e.name),e.rx&&i.attr("rx",e.rx),e.ry&&i.attr("ry",e.ry),void 0!==e.attrs)for(let t in e.attrs)i.attr(t,e.attrs[t]);return e.class&&i.attr("class",e.class),i},"drawRect"),n=(0,r.K2)((t,e)=>{s(t,{x:e.startx,y:e.starty,width:e.stopx-e.startx,height:e.stopy-e.starty,fill:e.fill,stroke:e.stroke,class:"rect"}).lower()},"drawBackgroundRect"),l=(0,r.K2)((t,e)=>{let i=e.text.replace(r.H1," "),a=t.append("text");a.attr("x",e.x),a.attr("y",e.y),a.attr("class","legend"),a.style("text-anchor",e.anchor),e.class&&a.attr("class",e.class);let s=a.append("tspan");return s.attr("x",e.x+2*e.textMargin),s.text(i),a},"drawText"),o=(0,r.K2)((t,e,i,r)=>{let s=t.append("image");s.attr("x",e),s.attr("y",i);let n=(0,a.J)(r);s.attr("xlink:href",n)},"drawImage"),c=(0,r.K2)((t,e,i,r)=>{let s=t.append("use");s.attr("x",e),s.attr("y",i);let n=(0,a.J)(r);s.attr("xlink:href",`#${n}`)},"drawEmbeddedImage"),h=(0,r.K2)(()=>({x:0,y:0,width:100,height:100,fill:"#EDF2AE",stroke:"#666",anchor:"start",rx:0,ry:0}),"getNoteRect"),u=(0,r.K2)(()=>({x:0,y:0,width:100,height:100,"text-anchor":"start",style:"#666",textMargin:0,rx:0,ry:0,tspan:!0}),"getTextObj")},85270:(t,e,i)=>{i.d(e,{diagram:()=>R});var r=i(95033),a=i(85354),s=i(9670),n=function(){var t=(0,a.K2)(function(t,e,i,r){for(i=i||{},r=t.length;r--;i[t[r]]=e);return i},"o"),e=[6,8,10,11,12,14,16,17,18],i=[1,9],r=[1,10],s=[1,11],n=[1,12],l=[1,13],o=[1,14],c={trace:(0,a.K2)(function(){},"trace"),yy:{},symbols_:{error:2,start:3,journey:4,document:5,EOF:6,line:7,SPACE:8,statement:9,NEWLINE:10,title:11,acc_title:12,acc_title_value:13,acc_descr:14,acc_descr_value:15,acc_descr_multiline_value:16,section:17,taskName:18,taskData:19,$accept:0,$end:1},terminals_:{2:"error",4:"journey",6:"EOF",8:"SPACE",10:"NEWLINE",11:"title",12:"acc_title",13:"acc_title_value",14:"acc_descr",15:"acc_descr_value",16:"acc_descr_multiline_value",17:"section",18:"taskName",19:"taskData"},productions_:[0,[3,3],[5,0],[5,2],[7,2],[7,1],[7,1],[7,1],[9,1],[9,2],[9,2],[9,1],[9,1],[9,2]],performAction:(0,a.K2)(function(t,e,i,r,a,s,n){var l=s.length-1;switch(a){case 1:return s[l-1];case 2:case 6:case 7:this.$=[];break;case 3:s[l-1].push(s[l]),this.$=s[l-1];break;case 4:case 5:this.$=s[l];break;case 8:r.setDiagramTitle(s[l].substr(6)),this.$=s[l].substr(6);break;case 9:this.$=s[l].trim(),r.setAccTitle(this.$);break;case 10:case 11:this.$=s[l].trim(),r.setAccDescription(this.$);break;case 12:r.addSection(s[l].substr(8)),this.$=s[l].substr(8);break;case 13:r.addTask(s[l-1],s[l]),this.$="task"}},"anonymous"),table:[{3:1,4:[1,2]},{1:[3]},t(e,[2,2],{5:3}),{6:[1,4],7:5,8:[1,6],9:7,10:[1,8],11:i,12:r,14:s,16:n,17:l,18:o},t(e,[2,7],{1:[2,1]}),t(e,[2,3]),{9:15,11:i,12:r,14:s,16:n,17:l,18:o},t(e,[2,5]),t(e,[2,6]),t(e,[2,8]),{13:[1,16]},{15:[1,17]},t(e,[2,11]),t(e,[2,12]),{19:[1,18]},t(e,[2,4]),t(e,[2,9]),t(e,[2,10]),t(e,[2,13])],defaultActions:{},parseError:(0,a.K2)(function(t,e){if(e.recoverable)this.trace(t);else{var i=Error(t);throw i.hash=e,i}},"parseError"),parse:(0,a.K2)(function(t){var e=this,i=[0],r=[],s=[null],n=[],l=this.table,o="",c=0,h=0,u=0,y=n.slice.call(arguments,1),p=Object.create(this.lexer),d={yy:{}};for(var f in this.yy)Object.prototype.hasOwnProperty.call(this.yy,f)&&(d.yy[f]=this.yy[f]);p.setInput(t,d.yy),d.yy.lexer=p,d.yy.parser=this,void 0===p.yylloc&&(p.yylloc={});var g=p.yylloc;n.push(g);var x=p.options&&p.options.ranges;function m(){var t;return"number"!=typeof(t=r.pop()||p.lex()||1)&&(t instanceof Array&&(t=(r=t).pop()),t=e.symbols_[t]||t),t}"function"==typeof d.yy.parseError?this.parseError=d.yy.parseError:this.parseError=Object.getPrototypeOf(this).parseError,(0,a.K2)(function(t){i.length=i.length-2*t,s.length=s.length-t,n.length=n.length-t},"popStack"),(0,a.K2)(m,"lex");for(var k,_,b,v,K,w,$,T,M,E={};;){if(b=i[i.length-1],this.defaultActions[b]?v=this.defaultActions[b]:(null==k&&(k=m()),v=l[b]&&l[b][k]),void 0===v||!v.length||!v[0]){var S="";for(w in M=[],l[b])this.terminals_[w]&&w>2&&M.push("'"+this.terminals_[w]+"'");S=p.showPosition?"Parse error on line "+(c+1)+":\n"+p.showPosition()+"\nExpecting "+M.join(", ")+", got '"+(this.terminals_[k]||k)+"'":"Parse error on line "+(c+1)+": Unexpected "+(1==k?"end of input":"'"+(this.terminals_[k]||k)+"'"),this.parseError(S,{text:p.match,token:this.terminals_[k]||k,line:p.yylineno,loc:g,expected:M})}if(v[0]instanceof Array&&v.length>1)throw Error("Parse Error: multiple actions possible at state: "+b+", token: "+k);switch(v[0]){case 1:i.push(k),s.push(p.yytext),n.push(p.yylloc),i.push(v[1]),k=null,_?(k=_,_=null):(h=p.yyleng,o=p.yytext,c=p.yylineno,g=p.yylloc,u>0&&u--);break;case 2:if($=this.productions_[v[1]][1],E.$=s[s.length-$],E._$={first_line:n[n.length-($||1)].first_line,last_line:n[n.length-1].last_line,first_column:n[n.length-($||1)].first_column,last_column:n[n.length-1].last_column},x&&(E._$.range=[n[n.length-($||1)].range[0],n[n.length-1].range[1]]),void 0!==(K=this.performAction.apply(E,[o,h,c,d.yy,v[1],s,n].concat(y))))return K;$&&(i=i.slice(0,-1*$*2),s=s.slice(0,-1*$),n=n.slice(0,-1*$)),i.push(this.productions_[v[1]][0]),s.push(E.$),n.push(E._$),T=l[i[i.length-2]][i[i.length-1]],i.push(T);break;case 3:return!0}}return!0},"parse")},h={EOF:1,parseError:(0,a.K2)(function(t,e){if(this.yy.parser)this.yy.parser.parseError(t,e);else throw Error(t)},"parseError"),setInput:(0,a.K2)(function(t,e){return this.yy=e||this.yy||{},this._input=t,this._more=this._backtrack=this.done=!1,this.yylineno=this.yyleng=0,this.yytext=this.matched=this.match="",this.conditionStack=["INITIAL"],this.yylloc={first_line:1,first_column:0,last_line:1,last_column:0},this.options.ranges&&(this.yylloc.range=[0,0]),this.offset=0,this},"setInput"),input:(0,a.K2)(function(){var t=this._input[0];return this.yytext+=t,this.yyleng++,this.offset++,this.match+=t,this.matched+=t,t.match(/(?:\r\n?|\n).*/g)?(this.yylineno++,this.yylloc.last_line++):this.yylloc.last_column++,this.options.ranges&&this.yylloc.range[1]++,this._input=this._input.slice(1),t},"input"),unput:(0,a.K2)(function(t){var e=t.length,i=t.split(/(?:\r\n?|\n)/g);this._input=t+this._input,this.yytext=this.yytext.substr(0,this.yytext.length-e),this.offset-=e;var r=this.match.split(/(?:\r\n?|\n)/g);this.match=this.match.substr(0,this.match.length-1),this.matched=this.matched.substr(0,this.matched.length-1),i.length-1&&(this.yylineno-=i.length-1);var a=this.yylloc.range;return this.yylloc={first_line:this.yylloc.first_line,last_line:this.yylineno+1,first_column:this.yylloc.first_column,last_column:i?(i.length===r.length?this.yylloc.first_column:0)+r[r.length-i.length].length-i[0].length:this.yylloc.first_column-e},this.options.ranges&&(this.yylloc.range=[a[0],a[0]+this.yyleng-e]),this.yyleng=this.yytext.length,this},"unput"),more:(0,a.K2)(function(){return this._more=!0,this},"more"),reject:(0,a.K2)(function(){return this.options.backtrack_lexer?(this._backtrack=!0,this):this.parseError("Lexical error on line "+(this.yylineno+1)+". You can only invoke reject() in the lexer when the lexer is of the backtracking persuasion (options.backtrack_lexer = true).\n"+this.showPosition(),{text:"",token:null,line:this.yylineno})},"reject"),less:(0,a.K2)(function(t){this.unput(this.match.slice(t))},"less"),pastInput:(0,a.K2)(function(){var t=this.matched.substr(0,this.matched.length-this.match.length);return(t.length>20?"...":"")+t.substr(-20).replace(/\n/g,"")},"pastInput"),upcomingInput:(0,a.K2)(function(){var t=this.match;return t.length<20&&(t+=this._input.substr(0,20-t.length)),(t.substr(0,20)+(t.length>20?"...":"")).replace(/\n/g,"")},"upcomingInput"),showPosition:(0,a.K2)(function(){var t=this.pastInput(),e=Array(t.length+1).join("-");return t+this.upcomingInput()+"\n"+e+"^"},"showPosition"),test_match:(0,a.K2)(function(t,e){var i,r,a;if(this.options.backtrack_lexer&&(a={yylineno:this.yylineno,yylloc:{first_line:this.yylloc.first_line,last_line:this.last_line,first_column:this.yylloc.first_column,last_column:this.yylloc.last_column},yytext:this.yytext,match:this.match,matches:this.matches,matched:this.matched,yyleng:this.yyleng,offset:this.offset,_more:this._more,_input:this._input,yy:this.yy,conditionStack:this.conditionStack.slice(0),done:this.done},this.options.ranges&&(a.yylloc.range=this.yylloc.range.slice(0))),(r=t[0].match(/(?:\r\n?|\n).*/g))&&(this.yylineno+=r.length),this.yylloc={first_line:this.yylloc.last_line,last_line:this.yylineno+1,first_column:this.yylloc.last_column,last_column:r?r[r.length-1].length-r[r.length-1].match(/\r?\n?/)[0].length:this.yylloc.last_column+t[0].length},this.yytext+=t[0],this.match+=t[0],this.matches=t,this.yyleng=this.yytext.length,this.options.ranges&&(this.yylloc.range=[this.offset,this.offset+=this.yyleng]),this._more=!1,this._backtrack=!1,this._input=this._input.slice(t[0].length),this.matched+=t[0],i=this.performAction.call(this,this.yy,this,e,this.conditionStack[this.conditionStack.length-1]),this.done&&this._input&&(this.done=!1),i)return i;if(this._backtrack)for(var s in a)this[s]=a[s];return!1},"test_match"),next:(0,a.K2)(function(){if(this.done)return this.EOF;this._input||(this.done=!0),this._more||(this.yytext="",this.match="");for(var t,e,i,r,a=this._currentRules(),s=0;s<a.length;s++)if((i=this._input.match(this.rules[a[s]]))&&(!e||i[0].length>e[0].length)){if(e=i,r=s,this.options.backtrack_lexer){if(!1!==(t=this.test_match(i,a[s])))return t;if(!this._backtrack)return!1;e=!1;continue}if(!this.options.flex)break}return e?!1!==(t=this.test_match(e,a[r]))&&t:""===this._input?this.EOF:this.parseError("Lexical error on line "+(this.yylineno+1)+". Unrecognized text.\n"+this.showPosition(),{text:"",token:null,line:this.yylineno})},"next"),lex:(0,a.K2)(function(){return this.next()||this.lex()},"lex"),begin:(0,a.K2)(function(t){this.conditionStack.push(t)},"begin"),popState:(0,a.K2)(function(){return this.conditionStack.length-1>0?this.conditionStack.pop():this.conditionStack[0]},"popState"),_currentRules:(0,a.K2)(function(){return this.conditionStack.length&&this.conditionStack[this.conditionStack.length-1]?this.conditions[this.conditionStack[this.conditionStack.length-1]].rules:this.conditions.INITIAL.rules},"_currentRules"),topState:(0,a.K2)(function(t){return(t=this.conditionStack.length-1-Math.abs(t||0))>=0?this.conditionStack[t]:"INITIAL"},"topState"),pushState:(0,a.K2)(function(t){this.begin(t)},"pushState"),stateStackSize:(0,a.K2)(function(){return this.conditionStack.length},"stateStackSize"),options:{"case-insensitive":!0},performAction:(0,a.K2)(function(t,e,i,r){switch(i){case 0:case 1:case 3:case 4:break;case 2:return 10;case 5:return 4;case 6:return 11;case 7:return this.begin("acc_title"),12;case 8:return this.popState(),"acc_title_value";case 9:return this.begin("acc_descr"),14;case 10:return this.popState(),"acc_descr_value";case 11:this.begin("acc_descr_multiline");break;case 12:this.popState();break;case 13:return"acc_descr_multiline_value";case 14:return 17;case 15:return 18;case 16:return 19;case 17:return":";case 18:return 6;case 19:return"INVALID"}},"anonymous"),rules:[/^(?:%(?!\{)[^\n]*)/i,/^(?:[^\}]%%[^\n]*)/i,/^(?:[\n]+)/i,/^(?:\s+)/i,/^(?:#[^\n]*)/i,/^(?:journey\b)/i,/^(?:title\s[^#\n;]+)/i,/^(?:accTitle\s*:\s*)/i,/^(?:(?!\n||)*[^\n]*)/i,/^(?:accDescr\s*:\s*)/i,/^(?:(?!\n||)*[^\n]*)/i,/^(?:accDescr\s*\{\s*)/i,/^(?:[\}])/i,/^(?:[^\}]*)/i,/^(?:section\s[^#:\n;]+)/i,/^(?:[^#:\n;]+)/i,/^(?::[^#\n;]+)/i,/^(?::)/i,/^(?:$)/i,/^(?:.)/i],conditions:{acc_descr_multiline:{rules:[12,13],inclusive:!1},acc_descr:{rules:[10],inclusive:!1},acc_title:{rules:[8],inclusive:!1},INITIAL:{rules:[0,1,2,3,4,5,6,7,9,11,14,15,16,17,18,19],inclusive:!0}}};function u(){this.yy={}}return c.lexer=h,(0,a.K2)(u,"Parser"),u.prototype=c,c.Parser=u,new u}();n.parser=n;var l="",o=[],c=[],h=[],u=(0,a.K2)(function(){o.length=0,c.length=0,l="",h.length=0,(0,a.IU)()},"clear"),y=(0,a.K2)(function(t){l=t,o.push(t)},"addSection"),p=(0,a.K2)(function(){return o},"getSections"),d=(0,a.K2)(function(){let t=m(),e=0;for(;!t&&e<100;)t=m(),e++;return c.push(...h),c},"getTasks"),f=(0,a.K2)(function(){let t=[];return c.forEach(e=>{e.people&&t.push(...e.people)}),[...new Set(t)].sort()},"updateActors"),g=(0,a.K2)(function(t,e){let i=e.substr(1).split(":"),r=0,a=[];1===i.length?(r=Number(i[0]),a=[]):(r=Number(i[0]),a=i[1].split(","));let s=a.map(t=>t.trim()),n={section:l,type:l,people:s,task:t,score:r};h.push(n)},"addTask"),x=(0,a.K2)(function(t){let e={section:l,type:l,description:t,task:t,classes:[]};c.push(e)},"addTaskOrg"),m=(0,a.K2)(function(){let t=(0,a.K2)(function(t){return h[t].processed},"compileTask"),e=!0;for(let[i,r]of h.entries())t(i),e=e&&r.processed;return e},"compileTasks"),k=(0,a.K2)(function(){return f()},"getActors"),_={getConfig:(0,a.K2)(()=>(0,a.D7)().journey,"getConfig"),clear:u,setDiagramTitle:a.ke,getDiagramTitle:a.ab,setAccTitle:a.SV,getAccTitle:a.iN,setAccDescription:a.EI,getAccDescription:a.m7,addSection:y,getSections:p,getTasks:d,addTask:g,addTaskOrg:x,getActors:k},b=(0,a.K2)(t=>`.label {
    font-family: 'trebuchet ms', verdana, arial, sans-serif;
    font-family: var(--mermaid-font-family);
    color: ${t.textColor};
  }
  .mouth {
    stroke: #666;
  }

  line {
    stroke: ${t.textColor}
  }

  .legend {
    fill: ${t.textColor};
  }

  .label text {
    fill: #333;
  }
  .label {
    color: ${t.textColor}
  }

  .face {
    ${t.faceColor?`fill: ${t.faceColor}`:"fill: #FFF8DC"};
    stroke: #999;
  }

  .node rect,
  .node circle,
  .node ellipse,
  .node polygon,
  .node path {
    fill: ${t.mainBkg};
    stroke: ${t.nodeBorder};
    stroke-width: 1px;
  }

  .node .label {
    text-align: center;
  }
  .node.clickable {
    cursor: pointer;
  }

  .arrowheadPath {
    fill: ${t.arrowheadColor};
  }

  .edgePath .path {
    stroke: ${t.lineColor};
    stroke-width: 1.5px;
  }

  .flowchart-link {
    stroke: ${t.lineColor};
    fill: none;
  }

  .edgeLabel {
    background-color: ${t.edgeLabelBackground};
    rect {
      opacity: 0.5;
    }
    text-align: center;
  }

  .cluster rect {
  }

  .cluster text {
    fill: ${t.titleColor};
  }

  div.mermaidTooltip {
    position: absolute;
    text-align: center;
    max-width: 200px;
    padding: 2px;
    font-family: 'trebuchet ms', verdana, arial, sans-serif;
    font-family: var(--mermaid-font-family);
    font-size: 12px;
    background: ${t.tertiaryColor};
    border: 1px solid ${t.border2};
    border-radius: 2px;
    pointer-events: none;
    z-index: 100;
  }

  .task-type-0, .section-type-0  {
    ${t.fillType0?`fill: ${t.fillType0}`:""};
  }
  .task-type-1, .section-type-1  {
    ${t.fillType0?`fill: ${t.fillType1}`:""};
  }
  .task-type-2, .section-type-2  {
    ${t.fillType0?`fill: ${t.fillType2}`:""};
  }
  .task-type-3, .section-type-3  {
    ${t.fillType0?`fill: ${t.fillType3}`:""};
  }
  .task-type-4, .section-type-4  {
    ${t.fillType0?`fill: ${t.fillType4}`:""};
  }
  .task-type-5, .section-type-5  {
    ${t.fillType0?`fill: ${t.fillType5}`:""};
  }
  .task-type-6, .section-type-6  {
    ${t.fillType0?`fill: ${t.fillType6}`:""};
  }
  .task-type-7, .section-type-7  {
    ${t.fillType0?`fill: ${t.fillType7}`:""};
  }

  .actor-0 {
    ${t.actor0?`fill: ${t.actor0}`:""};
  }
  .actor-1 {
    ${t.actor1?`fill: ${t.actor1}`:""};
  }
  .actor-2 {
    ${t.actor2?`fill: ${t.actor2}`:""};
  }
  .actor-3 {
    ${t.actor3?`fill: ${t.actor3}`:""};
  }
  .actor-4 {
    ${t.actor4?`fill: ${t.actor4}`:""};
  }
  .actor-5 {
    ${t.actor5?`fill: ${t.actor5}`:""};
  }
`,"getStyles"),v=(0,a.K2)(function(t,e){return(0,r.tk)(t,e)},"drawRect"),K=(0,a.K2)(function(t,e){let i=t.append("circle").attr("cx",e.cx).attr("cy",e.cy).attr("class","face").attr("r",15).attr("stroke-width",2).attr("overflow","visible"),r=t.append("g");function n(t){let i=(0,s.JLW)().startAngle(Math.PI/2).endAngle(Math.PI/2*3).innerRadius(7.5).outerRadius(15/2.2);t.append("path").attr("class","mouth").attr("d",i).attr("transform","translate("+e.cx+","+(e.cy+2)+")")}function l(t){let i=(0,s.JLW)().startAngle(3*Math.PI/2).endAngle(Math.PI/2*5).innerRadius(7.5).outerRadius(15/2.2);t.append("path").attr("class","mouth").attr("d",i).attr("transform","translate("+e.cx+","+(e.cy+7)+")")}function o(t){t.append("line").attr("class","mouth").attr("stroke",2).attr("x1",e.cx-5).attr("y1",e.cy+7).attr("x2",e.cx+5).attr("y2",e.cy+7).attr("class","mouth").attr("stroke-width","1px").attr("stroke","#666")}return r.append("circle").attr("cx",e.cx-5).attr("cy",e.cy-5).attr("r",1.5).attr("stroke-width",2).attr("fill","#666").attr("stroke","#666"),r.append("circle").attr("cx",e.cx+5).attr("cy",e.cy-5).attr("r",1.5).attr("stroke-width",2).attr("fill","#666").attr("stroke","#666"),(0,a.K2)(n,"smile"),(0,a.K2)(l,"sad"),(0,a.K2)(o,"ambivalent"),e.score>3?n(r):e.score<3?l(r):o(r),i},"drawFace"),w=(0,a.K2)(function(t,e){let i=t.append("circle");return i.attr("cx",e.cx),i.attr("cy",e.cy),i.attr("class","actor-"+e.pos),i.attr("fill",e.fill),i.attr("stroke",e.stroke),i.attr("r",e.r),void 0!==i.class&&i.attr("class",i.class),void 0!==e.title&&i.append("title").text(e.title),i},"drawCircle"),$=(0,a.K2)(function(t,e){return(0,r.m)(t,e)},"drawText"),T=(0,a.K2)(function(t,e,i){let a=t.append("g"),s=(0,r.PB)();s.x=e.x,s.y=e.y,s.fill=e.fill,s.width=i.width*e.taskCount+i.diagramMarginX*(e.taskCount-1),s.height=i.height,s.class="journey-section section-type-"+e.num,s.rx=3,s.ry=3,v(a,s),S(i)(e.text,a,s.x,s.y,s.width,s.height,{class:"journey-section section-type-"+e.num},i,e.colour)},"drawSection"),M=-1,E=(0,a.K2)(function(t,e,i){let a=e.x+i.width/2,s=t.append("g");M++,s.append("line").attr("id","task"+M).attr("x1",a).attr("y1",e.y).attr("x2",a).attr("y2",450).attr("class","task-line").attr("stroke-width","1px").attr("stroke-dasharray","4 2").attr("stroke","#666"),K(s,{cx:a,cy:300+(5-e.score)*30,score:e.score});let n=(0,r.PB)();n.x=e.x,n.y=e.y,n.fill=e.fill,n.width=i.width,n.height=i.height,n.class="task task-type-"+e.num,n.rx=3,n.ry=3,v(s,n);let l=e.x+14;e.people.forEach(t=>{let i=e.actors[t].color;w(s,{cx:l,cy:e.y,r:7,fill:i,stroke:"#000",title:t,pos:e.actors[t].position}),l+=10}),S(i)(e.task,s,n.x,n.y,n.width,n.height,{class:"task"},i,e.colour)},"drawTask"),S=function(){function t(t,e,i,a,s,n,l,o){r(e.append("text").attr("x",i+s/2).attr("y",a+n/2+5).style("font-color",o).style("text-anchor","middle").text(t),l)}function e(t,e,i,a,s,n,l,o,c){let{taskFontSize:h,taskFontFamily:u}=o,y=t.split(/<br\s*\/?>/gi);for(let t=0;t<y.length;t++){let o=t*h-h*(y.length-1)/2,p=e.append("text").attr("x",i+s/2).attr("y",a).attr("fill",c).style("text-anchor","middle").style("font-size",h).style("font-family",u);p.append("tspan").attr("x",i+s/2).attr("dy",o).text(y[t]),p.attr("y",a+n/2).attr("dominant-baseline","central").attr("alignment-baseline","central"),r(p,l)}}function i(t,i,a,s,n,l,o,c){let h=i.append("switch"),u=h.append("foreignObject").attr("x",a).attr("y",s).attr("width",n).attr("height",l).attr("position","fixed").append("xhtml:div").style("display","table").style("height","100%").style("width","100%");u.append("div").attr("class","label").style("display","table-cell").style("text-align","center").style("vertical-align","middle").text(t),e(t,h,a,s,n,l,o,c),r(u,o)}function r(t,e){for(let i in e)i in e&&t.attr(i,e[i])}return(0,a.K2)(t,"byText"),(0,a.K2)(e,"byTspan"),(0,a.K2)(i,"byFo"),(0,a.K2)(r,"_setTextAttrs"),function(r){return"fo"===r.textPlacement?i:"old"===r.textPlacement?t:e}}(),I={drawCircle:w,drawSection:T,drawText:$,drawTask:E,initGraphics:(0,a.K2)(function(t){t.append("defs").append("marker").attr("id","arrowhead").attr("refX",5).attr("refY",2).attr("markerWidth",6).attr("markerHeight",4).attr("orient","auto").append("path").attr("d","M 0,0 V 4 L6,2 Z")},"initGraphics")},P=(0,a.K2)(function(t){Object.keys(t).forEach(function(e){j[e]=t[e]})},"setConf"),A={};function C(t){let e=(0,a.D7)().journey,i=60;Object.keys(A).forEach(r=>{let a=A[r].color,s={cx:20,cy:i,r:7,fill:a,stroke:"#000",pos:A[r].position};I.drawCircle(t,s);let n={x:40,y:i+7,fill:"#666",text:r,textMargin:5|e.boxTextMargin};I.drawText(t,n),i+=20})}(0,a.K2)(C,"drawActorLegend");var j=(0,a.D7)().journey,D=j.leftMargin,V=(0,a.K2)(function(t,e,i,r){let n;let l=(0,a.D7)().journey,o=(0,a.D7)().securityLevel;"sandbox"===o&&(n=(0,s.Ltv)("#i"+e));let c="sandbox"===o?(0,s.Ltv)(n.nodes()[0].contentDocument.body):(0,s.Ltv)("body");L.init();let h=c.select("#"+e);I.initGraphics(h);let u=r.db.getTasks(),y=r.db.getDiagramTitle(),p=r.db.getActors();for(let t in A)delete A[t];let d=0;p.forEach(t=>{A[t]={color:l.actorColours[d%l.actorColours.length],position:d},d++}),C(h),L.insert(0,0,D,50*Object.keys(A).length),B(h,u,0);let f=L.getBounds();y&&h.append("text").text(y).attr("x",D).attr("font-size","4ex").attr("font-weight","bold").attr("y",25);let g=f.stopy-f.starty+2*l.diagramMarginY,x=D+f.stopx+2*l.diagramMarginX;(0,a.a$)(h,g,x,l.useMaxWidth),h.append("line").attr("x1",D).attr("y1",4*l.height).attr("x2",x-D-4).attr("y2",4*l.height).attr("stroke-width",4).attr("stroke","black").attr("marker-end","url(#arrowhead)");let m=y?70:0;h.attr("viewBox",`${f.startx} -25 ${x} ${g+m}`),h.attr("preserveAspectRatio","xMinYMin meet"),h.attr("height",g+m+25)},"draw"),L={data:{startx:void 0,stopx:void 0,starty:void 0,stopy:void 0},verticalPos:0,sequenceItems:[],init:(0,a.K2)(function(){this.sequenceItems=[],this.data={startx:void 0,stopx:void 0,starty:void 0,stopy:void 0},this.verticalPos=0},"init"),updateVal:(0,a.K2)(function(t,e,i,r){void 0===t[e]?t[e]=i:t[e]=r(i,t[e])},"updateVal"),updateBounds:(0,a.K2)(function(t,e,i,r){let s=(0,a.D7)().journey,n=this,l=0;function o(o){return(0,a.K2)(function(a){l++;let c=n.sequenceItems.length-l+1;n.updateVal(a,"starty",e-c*s.boxMargin,Math.min),n.updateVal(a,"stopy",r+c*s.boxMargin,Math.max),n.updateVal(L.data,"startx",t-c*s.boxMargin,Math.min),n.updateVal(L.data,"stopx",i+c*s.boxMargin,Math.max),"activation"!==o&&(n.updateVal(a,"startx",t-c*s.boxMargin,Math.min),n.updateVal(a,"stopx",i+c*s.boxMargin,Math.max),n.updateVal(L.data,"starty",e-c*s.boxMargin,Math.min),n.updateVal(L.data,"stopy",r+c*s.boxMargin,Math.max))},"updateItemBounds")}(0,a.K2)(o,"updateFn"),this.sequenceItems.forEach(o())},"updateBounds"),insert:(0,a.K2)(function(t,e,i,r){let a=Math.min(t,i),s=Math.max(t,i),n=Math.min(e,r),l=Math.max(e,r);this.updateVal(L.data,"startx",a,Math.min),this.updateVal(L.data,"starty",n,Math.min),this.updateVal(L.data,"stopx",s,Math.max),this.updateVal(L.data,"stopy",l,Math.max),this.updateBounds(a,n,s,l)},"insert"),bumpVerticalPos:(0,a.K2)(function(t){this.verticalPos=this.verticalPos+t,this.data.stopy=this.verticalPos},"bumpVerticalPos"),getVerticalPos:(0,a.K2)(function(){return this.verticalPos},"getVerticalPos"),getBounds:(0,a.K2)(function(){return this.data},"getBounds")},N=j.sectionFills,O=j.sectionColours,B=(0,a.K2)(function(t,e,i){let r=(0,a.D7)().journey,s="",n=i+(2*r.height+r.diagramMarginY),l=0,o="#CCC",c="black",h=0;for(let[i,a]of e.entries()){if(s!==a.section){o=N[l%N.length],h=l%N.length,c=O[l%O.length];let n=0,u=a.section;for(let t=i;t<e.length;t++)if(e[t].section==u)n+=1;else break;let y={x:i*r.taskMargin+i*r.width+D,y:50,text:a.section,fill:o,num:h,colour:c,taskCount:n};I.drawSection(t,y,r),s=a.section,l++}let u=a.people.reduce((t,e)=>(A[e]&&(t[e]=A[e]),t),{});a.x=i*r.taskMargin+i*r.width+D,a.y=n,a.width=r.diagramMarginX,a.height=r.diagramMarginY,a.colour=c,a.fill=o,a.num=h,a.actors=u,I.drawTask(t,a,r),L.insert(a.x,a.y,a.x+a.width+r.taskMargin,450)}},"drawTasks"),F={setConf:P,draw:V},R={parser:n,db:_,renderer:F,styles:b,init:(0,a.K2)(t=>{F.setConf(t.journey),_.clear()},"init")}}}]);
