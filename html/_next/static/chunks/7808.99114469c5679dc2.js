"use strict";(self.webpackChunk_N_E=self.webpackChunk_N_E||[]).push([[7808],{7808:(t,e,i)=>{i.d(e,{diagram:()=>M});var n=i(85354),s=i(9670),r=i(13263),a=i(18202),o=i(56048),l=function(){var t=(0,n.K2)(function(t,e,i,n){for(i=i||{},n=t.length;n--;i[t[n]]=e);return i},"o"),e=[6,8,10,11,12,14,16,17,20,21],i=[1,9],s=[1,10],r=[1,11],a=[1,12],o=[1,13],l=[1,16],c=[1,17],h={trace:(0,n.K2)(function(){},"trace"),yy:{},symbols_:{error:2,start:3,timeline:4,document:5,EOF:6,line:7,SPACE:8,statement:9,NEWLINE:10,title:11,acc_title:12,acc_title_value:13,acc_descr:14,acc_descr_value:15,acc_descr_multiline_value:16,section:17,period_statement:18,event_statement:19,period:20,event:21,$accept:0,$end:1},terminals_:{2:"error",4:"timeline",6:"EOF",8:"SPACE",10:"NEWLINE",11:"title",12:"acc_title",13:"acc_title_value",14:"acc_descr",15:"acc_descr_value",16:"acc_descr_multiline_value",17:"section",20:"period",21:"event"},productions_:[0,[3,3],[5,0],[5,2],[7,2],[7,1],[7,1],[7,1],[9,1],[9,2],[9,2],[9,1],[9,1],[9,1],[9,1],[18,1],[19,1]],performAction:(0,n.K2)(function(t,e,i,n,s,r,a){var o=r.length-1;switch(s){case 1:return r[o-1];case 2:case 6:case 7:this.$=[];break;case 3:r[o-1].push(r[o]),this.$=r[o-1];break;case 4:case 5:this.$=r[o];break;case 8:n.getCommonDb().setDiagramTitle(r[o].substr(6)),this.$=r[o].substr(6);break;case 9:this.$=r[o].trim(),n.getCommonDb().setAccTitle(this.$);break;case 10:case 11:this.$=r[o].trim(),n.getCommonDb().setAccDescription(this.$);break;case 12:n.addSection(r[o].substr(8)),this.$=r[o].substr(8);break;case 15:n.addTask(r[o],0,""),this.$=r[o];break;case 16:n.addEvent(r[o].substr(2)),this.$=r[o]}},"anonymous"),table:[{3:1,4:[1,2]},{1:[3]},t(e,[2,2],{5:3}),{6:[1,4],7:5,8:[1,6],9:7,10:[1,8],11:i,12:s,14:r,16:a,17:o,18:14,19:15,20:l,21:c},t(e,[2,7],{1:[2,1]}),t(e,[2,3]),{9:18,11:i,12:s,14:r,16:a,17:o,18:14,19:15,20:l,21:c},t(e,[2,5]),t(e,[2,6]),t(e,[2,8]),{13:[1,19]},{15:[1,20]},t(e,[2,11]),t(e,[2,12]),t(e,[2,13]),t(e,[2,14]),t(e,[2,15]),t(e,[2,16]),t(e,[2,4]),t(e,[2,9]),t(e,[2,10])],defaultActions:{},parseError:(0,n.K2)(function(t,e){if(e.recoverable)this.trace(t);else{var i=Error(t);throw i.hash=e,i}},"parseError"),parse:(0,n.K2)(function(t){var e=this,i=[0],s=[],r=[null],a=[],o=this.table,l="",c=0,h=0,d=0,u=a.slice.call(arguments,1),p=Object.create(this.lexer),g={yy:{}};for(var y in this.yy)Object.prototype.hasOwnProperty.call(this.yy,y)&&(g.yy[y]=this.yy[y]);p.setInput(t,g.yy),g.yy.lexer=p,g.yy.parser=this,void 0===p.yylloc&&(p.yylloc={});var m=p.yylloc;a.push(m);var f=p.options&&p.options.ranges;function _(){var t;return"number"!=typeof(t=s.pop()||p.lex()||1)&&(t instanceof Array&&(t=(s=t).pop()),t=e.symbols_[t]||t),t}"function"==typeof g.yy.parseError?this.parseError=g.yy.parseError:this.parseError=Object.getPrototypeOf(this).parseError,(0,n.K2)(function(t){i.length=i.length-2*t,r.length=r.length-t,a.length=a.length-t},"popStack"),(0,n.K2)(_,"lex");for(var b,k,x,v,S,w,K,$,E,I={};;){if(x=i[i.length-1],this.defaultActions[x]?v=this.defaultActions[x]:(null==b&&(b=_()),v=o[x]&&o[x][b]),void 0===v||!v.length||!v[0]){var T="";for(w in E=[],o[x])this.terminals_[w]&&w>2&&E.push("'"+this.terminals_[w]+"'");T=p.showPosition?"Parse error on line "+(c+1)+":\n"+p.showPosition()+"\nExpecting "+E.join(", ")+", got '"+(this.terminals_[b]||b)+"'":"Parse error on line "+(c+1)+": Unexpected "+(1==b?"end of input":"'"+(this.terminals_[b]||b)+"'"),this.parseError(T,{text:p.match,token:this.terminals_[b]||b,line:p.yylineno,loc:m,expected:E})}if(v[0]instanceof Array&&v.length>1)throw Error("Parse Error: multiple actions possible at state: "+x+", token: "+b);switch(v[0]){case 1:i.push(b),r.push(p.yytext),a.push(p.yylloc),i.push(v[1]),b=null,k?(b=k,k=null):(h=p.yyleng,l=p.yytext,c=p.yylineno,m=p.yylloc,d>0&&d--);break;case 2:if(K=this.productions_[v[1]][1],I.$=r[r.length-K],I._$={first_line:a[a.length-(K||1)].first_line,last_line:a[a.length-1].last_line,first_column:a[a.length-(K||1)].first_column,last_column:a[a.length-1].last_column},f&&(I._$.range=[a[a.length-(K||1)].range[0],a[a.length-1].range[1]]),void 0!==(S=this.performAction.apply(I,[l,h,c,g.yy,v[1],r,a].concat(u))))return S;K&&(i=i.slice(0,-1*K*2),r=r.slice(0,-1*K),a=a.slice(0,-1*K)),i.push(this.productions_[v[1]][0]),r.push(I.$),a.push(I._$),$=o[i[i.length-2]][i[i.length-1]],i.push($);break;case 3:return!0}}return!0},"parse")},d={EOF:1,parseError:(0,n.K2)(function(t,e){if(this.yy.parser)this.yy.parser.parseError(t,e);else throw Error(t)},"parseError"),setInput:(0,n.K2)(function(t,e){return this.yy=e||this.yy||{},this._input=t,this._more=this._backtrack=this.done=!1,this.yylineno=this.yyleng=0,this.yytext=this.matched=this.match="",this.conditionStack=["INITIAL"],this.yylloc={first_line:1,first_column:0,last_line:1,last_column:0},this.options.ranges&&(this.yylloc.range=[0,0]),this.offset=0,this},"setInput"),input:(0,n.K2)(function(){var t=this._input[0];return this.yytext+=t,this.yyleng++,this.offset++,this.match+=t,this.matched+=t,t.match(/(?:\r\n?|\n).*/g)?(this.yylineno++,this.yylloc.last_line++):this.yylloc.last_column++,this.options.ranges&&this.yylloc.range[1]++,this._input=this._input.slice(1),t},"input"),unput:(0,n.K2)(function(t){var e=t.length,i=t.split(/(?:\r\n?|\n)/g);this._input=t+this._input,this.yytext=this.yytext.substr(0,this.yytext.length-e),this.offset-=e;var n=this.match.split(/(?:\r\n?|\n)/g);this.match=this.match.substr(0,this.match.length-1),this.matched=this.matched.substr(0,this.matched.length-1),i.length-1&&(this.yylineno-=i.length-1);var s=this.yylloc.range;return this.yylloc={first_line:this.yylloc.first_line,last_line:this.yylineno+1,first_column:this.yylloc.first_column,last_column:i?(i.length===n.length?this.yylloc.first_column:0)+n[n.length-i.length].length-i[0].length:this.yylloc.first_column-e},this.options.ranges&&(this.yylloc.range=[s[0],s[0]+this.yyleng-e]),this.yyleng=this.yytext.length,this},"unput"),more:(0,n.K2)(function(){return this._more=!0,this},"more"),reject:(0,n.K2)(function(){return this.options.backtrack_lexer?(this._backtrack=!0,this):this.parseError("Lexical error on line "+(this.yylineno+1)+". You can only invoke reject() in the lexer when the lexer is of the backtracking persuasion (options.backtrack_lexer = true).\n"+this.showPosition(),{text:"",token:null,line:this.yylineno})},"reject"),less:(0,n.K2)(function(t){this.unput(this.match.slice(t))},"less"),pastInput:(0,n.K2)(function(){var t=this.matched.substr(0,this.matched.length-this.match.length);return(t.length>20?"...":"")+t.substr(-20).replace(/\n/g,"")},"pastInput"),upcomingInput:(0,n.K2)(function(){var t=this.match;return t.length<20&&(t+=this._input.substr(0,20-t.length)),(t.substr(0,20)+(t.length>20?"...":"")).replace(/\n/g,"")},"upcomingInput"),showPosition:(0,n.K2)(function(){var t=this.pastInput(),e=Array(t.length+1).join("-");return t+this.upcomingInput()+"\n"+e+"^"},"showPosition"),test_match:(0,n.K2)(function(t,e){var i,n,s;if(this.options.backtrack_lexer&&(s={yylineno:this.yylineno,yylloc:{first_line:this.yylloc.first_line,last_line:this.last_line,first_column:this.yylloc.first_column,last_column:this.yylloc.last_column},yytext:this.yytext,match:this.match,matches:this.matches,matched:this.matched,yyleng:this.yyleng,offset:this.offset,_more:this._more,_input:this._input,yy:this.yy,conditionStack:this.conditionStack.slice(0),done:this.done},this.options.ranges&&(s.yylloc.range=this.yylloc.range.slice(0))),(n=t[0].match(/(?:\r\n?|\n).*/g))&&(this.yylineno+=n.length),this.yylloc={first_line:this.yylloc.last_line,last_line:this.yylineno+1,first_column:this.yylloc.last_column,last_column:n?n[n.length-1].length-n[n.length-1].match(/\r?\n?/)[0].length:this.yylloc.last_column+t[0].length},this.yytext+=t[0],this.match+=t[0],this.matches=t,this.yyleng=this.yytext.length,this.options.ranges&&(this.yylloc.range=[this.offset,this.offset+=this.yyleng]),this._more=!1,this._backtrack=!1,this._input=this._input.slice(t[0].length),this.matched+=t[0],i=this.performAction.call(this,this.yy,this,e,this.conditionStack[this.conditionStack.length-1]),this.done&&this._input&&(this.done=!1),i)return i;if(this._backtrack)for(var r in s)this[r]=s[r];return!1},"test_match"),next:(0,n.K2)(function(){if(this.done)return this.EOF;this._input||(this.done=!0),this._more||(this.yytext="",this.match="");for(var t,e,i,n,s=this._currentRules(),r=0;r<s.length;r++)if((i=this._input.match(this.rules[s[r]]))&&(!e||i[0].length>e[0].length)){if(e=i,n=r,this.options.backtrack_lexer){if(!1!==(t=this.test_match(i,s[r])))return t;if(!this._backtrack)return!1;e=!1;continue}if(!this.options.flex)break}return e?!1!==(t=this.test_match(e,s[n]))&&t:""===this._input?this.EOF:this.parseError("Lexical error on line "+(this.yylineno+1)+". Unrecognized text.\n"+this.showPosition(),{text:"",token:null,line:this.yylineno})},"next"),lex:(0,n.K2)(function(){return this.next()||this.lex()},"lex"),begin:(0,n.K2)(function(t){this.conditionStack.push(t)},"begin"),popState:(0,n.K2)(function(){return this.conditionStack.length-1>0?this.conditionStack.pop():this.conditionStack[0]},"popState"),_currentRules:(0,n.K2)(function(){return this.conditionStack.length&&this.conditionStack[this.conditionStack.length-1]?this.conditions[this.conditionStack[this.conditionStack.length-1]].rules:this.conditions.INITIAL.rules},"_currentRules"),topState:(0,n.K2)(function(t){return(t=this.conditionStack.length-1-Math.abs(t||0))>=0?this.conditionStack[t]:"INITIAL"},"topState"),pushState:(0,n.K2)(function(t){this.begin(t)},"pushState"),stateStackSize:(0,n.K2)(function(){return this.conditionStack.length},"stateStackSize"),options:{"case-insensitive":!0},performAction:(0,n.K2)(function(t,e,i,n){switch(i){case 0:case 1:case 3:case 4:break;case 2:return 10;case 5:return 4;case 6:return 11;case 7:return this.begin("acc_title"),12;case 8:return this.popState(),"acc_title_value";case 9:return this.begin("acc_descr"),14;case 10:return this.popState(),"acc_descr_value";case 11:this.begin("acc_descr_multiline");break;case 12:this.popState();break;case 13:return"acc_descr_multiline_value";case 14:return 17;case 15:return 21;case 16:return 20;case 17:return 6;case 18:return"INVALID"}},"anonymous"),rules:[/^(?:%(?!\{)[^\n]*)/i,/^(?:[^\}]%%[^\n]*)/i,/^(?:[\n]+)/i,/^(?:\s+)/i,/^(?:#[^\n]*)/i,/^(?:timeline\b)/i,/^(?:title\s[^\n]+)/i,/^(?:accTitle\s*:\s*)/i,/^(?:(?!\n||)*[^\n]*)/i,/^(?:accDescr\s*:\s*)/i,/^(?:(?!\n||)*[^\n]*)/i,/^(?:accDescr\s*\{\s*)/i,/^(?:[\}])/i,/^(?:[^\}]*)/i,/^(?:section\s[^:\n]+)/i,/^(?::\s[^:\n]+)/i,/^(?:[^#:\n]+)/i,/^(?:$)/i,/^(?:.)/i],conditions:{acc_descr_multiline:{rules:[12,13],inclusive:!1},acc_descr:{rules:[10],inclusive:!1},acc_title:{rules:[8],inclusive:!1},INITIAL:{rules:[0,1,2,3,4,5,6,7,9,11,14,15,16,17,18],inclusive:!0}}};function u(){this.yy={}}return h.lexer=d,(0,n.K2)(u,"Parser"),u.prototype=h,h.Parser=u,new u}();l.parser=l;var c={};(0,n.VA)(c,{addEvent:()=>x,addSection:()=>f,addTask:()=>k,addTaskOrg:()=>v,clear:()=>m,default:()=>w,getCommonDb:()=>y,getSections:()=>_,getTasks:()=>b});var h="",d=0,u=[],p=[],g=[],y=(0,n.K2)(()=>n.Wt,"getCommonDb"),m=(0,n.K2)(function(){u.length=0,p.length=0,h="",g.length=0,(0,n.IU)()},"clear"),f=(0,n.K2)(function(t){h=t,u.push(t)},"addSection"),_=(0,n.K2)(function(){return u},"getSections"),b=(0,n.K2)(function(){let t=S(),e=0;for(;!t&&e<100;)t=S(),e++;return p.push(...g),p},"getTasks"),k=(0,n.K2)(function(t,e,i){let n={id:d++,section:h,type:h,task:t,score:e||0,events:i?[i]:[]};g.push(n)},"addTask"),x=(0,n.K2)(function(t){g.find(t=>t.id===d-1).events.push(t)},"addEvent"),v=(0,n.K2)(function(t){let e={section:h,type:h,description:t,task:t,classes:[]};p.push(e)},"addTaskOrg"),S=(0,n.K2)(function(){let t=(0,n.K2)(function(t){return g[t].processed},"compileTask"),e=!0;for(let[i,n]of g.entries())t(i),e=e&&n.processed;return e},"compileTasks"),w={clear:m,getCommonDb:y,addSection:f,getSections:_,getTasks:b,addTask:k,addTaskOrg:v,addEvent:x},K=(0,n.K2)(function(t){t.append("defs").append("marker").attr("id","arrowhead").attr("refX",5).attr("refY",2).attr("markerWidth",6).attr("markerHeight",4).attr("orient","auto").append("path").attr("d","M 0,0 V 4 L6,2 Z")},"initGraphics");function $(t,e){t.each(function(){var t,i=(0,s.Ltv)(this),n=i.text().split(/(\s+|<br>)/).reverse(),r=[],a=i.attr("y"),o=parseFloat(i.attr("dy")),l=i.text(null).append("tspan").attr("x",0).attr("y",a).attr("dy",o+"em");for(let s=0;s<n.length;s++)t=n[n.length-1-s],r.push(t),l.text(r.join(" ").trim()),(l.node().getComputedTextLength()>e||"<br>"===t)&&(r.pop(),l.text(r.join(" ").trim()),r="<br>"===t?[""]:[t],l=i.append("tspan").attr("x",0).attr("y",a).attr("dy","1.1em").text(t))})}(0,n.K2)($,"wrap");var E=(0,n.K2)(function(t,e,i,n){let s=i%12-1,r=t.append("g");e.section=s,r.attr("class",(e.class?e.class+" ":"")+"timeline-node section-"+s);let a=r.append("g"),o=r.append("g"),l=o.append("text").text(e.descr).attr("dy","1em").attr("alignment-baseline","middle").attr("dominant-baseline","middle").attr("text-anchor","middle").call($,e.width).node().getBBox(),c=n.fontSize?.replace?n.fontSize.replace("px",""):n.fontSize;return e.height=l.height+.55*c+e.padding,e.height=Math.max(e.height,e.maxHeight),e.width=e.width+2*e.padding,o.attr("transform","translate("+e.width/2+", "+e.padding/2+")"),T(a,e,s,n),e},"drawNode"),I=(0,n.K2)(function(t,e,i){let n=t.append("g"),s=n.append("text").text(e.descr).attr("dy","1em").attr("alignment-baseline","middle").attr("dominant-baseline","middle").attr("text-anchor","middle").call($,e.width).node().getBBox(),r=i.fontSize?.replace?i.fontSize.replace("px",""):i.fontSize;return n.remove(),s.height+.55*r+e.padding},"getVirtualNodeHeight"),T=(0,n.K2)(function(t,e,i){t.append("path").attr("id","node-"+e.id).attr("class","node-bkg node-"+e.type).attr("d",`M0 ${e.height-5} v${-e.height+10} q0,-5 5,-5 h${e.width-10} q5,0 5,5 v${e.height-5} H0 Z`),t.append("line").attr("class","node-line-"+i).attr("x1",0).attr("y1",e.height).attr("x2",e.width).attr("y2",e.height)},"defaultBkg"),L={initGraphics:K,drawNode:E,getVirtualNodeHeight:I},A=(0,n.K2)(function(t,e,i,r){let a;let o=(0,n.D7)(),l=o.leftMargin??50;n.Rm.debug("timeline",r.db);let c=o.securityLevel;"sandbox"===c&&(a=(0,s.Ltv)("#i"+e));let h=("sandbox"===c?(0,s.Ltv)(a.nodes()[0].contentDocument.body):(0,s.Ltv)("body")).select("#"+e);h.append("g");let d=r.db.getTasks(),u=r.db.getCommonDb().getDiagramTitle();n.Rm.debug("task",d),L.initGraphics(h);let p=r.db.getSections();n.Rm.debug("sections",p);let g=0,y=0,m=0,f=0,_=50+l,b=50;f=50;let k=0,x=!0;p.forEach(function(t){let e={number:k,descr:t,section:k,width:150,padding:20,maxHeight:g},i=L.getVirtualNodeHeight(h,e,o);n.Rm.debug("sectionHeight before draw",i),g=Math.max(g,i+20)});let v=0,S=0;for(let[t,e]of(n.Rm.debug("tasks.length",d.length),d.entries())){let i={number:t,descr:e,section:e.section,width:150,padding:20,maxHeight:y},s=L.getVirtualNodeHeight(h,i,o);n.Rm.debug("taskHeight before draw",s),y=Math.max(y,s+20),v=Math.max(v,e.events.length);let r=0;for(let t of e.events){let i={descr:t,section:e.section,number:e.section,width:150,padding:20,maxHeight:50};r+=L.getVirtualNodeHeight(h,i,o)}S=Math.max(S,r)}n.Rm.debug("maxSectionHeight before draw",g),n.Rm.debug("maxTaskHeight before draw",y),p&&p.length>0?p.forEach(t=>{let e=d.filter(e=>e.section===t),i={number:k,descr:t,section:k,width:200*Math.max(e.length,1)-50,padding:20,maxHeight:g};n.Rm.debug("sectionNode",i);let s=h.append("g"),r=L.drawNode(s,i,k,o);n.Rm.debug("sectionNode output",r),s.attr("transform",`translate(${_}, ${f})`),b+=g+50,e.length>0&&N(h,e,k,_,b,y,o,v,S,g,!1),_+=200*Math.max(e.length,1),b=f,k++}):(x=!1,N(h,d,k,_,b,y,o,v,S,g,!0));let w=h.node().getBBox();n.Rm.debug("bounds",w),u&&h.append("text").text(u).attr("x",w.width/2-l).attr("font-size","4ex").attr("font-weight","bold").attr("y",20),m=x?g+y+150:y+100,h.append("g").attr("class","lineWrapper").append("line").attr("x1",l).attr("y1",m).attr("x2",w.width+3*l).attr("y2",m).attr("stroke-width",4).attr("stroke","black").attr("marker-end","url(#arrowhead)"),(0,n.ot)(void 0,h,o.timeline?.padding??50,o.timeline?.useMaxWidth??!1)},"draw"),N=(0,n.K2)(function(t,e,i,s,r,a,o,l,c,h,d){for(let l of e){let e={descr:l.task,section:i,number:i,width:150,padding:20,maxHeight:a};n.Rm.debug("taskNode",e);let u=t.append("g").attr("class","taskWrapper"),p=L.drawNode(u,e,i,o).height;if(n.Rm.debug("taskHeight after draw",p),u.attr("transform",`translate(${s}, ${r})`),a=Math.max(a,p),l.events){let e=t.append("g").attr("class","lineWrapper");r+=100,C(t,l.events,i,s,r,o),r-=100,e.append("line").attr("x1",s+95).attr("y1",r+a).attr("x2",s+95).attr("y2",r+a+(d?a:h)+c+120).attr("stroke-width",2).attr("stroke","black").attr("marker-end","url(#arrowhead)").attr("stroke-dasharray","5,5")}s+=200,d&&!o.timeline?.disableMulticolor&&i++}},"drawTasks"),C=(0,n.K2)(function(t,e,i,s,r,a){let o=0,l=r;for(let l of(r+=100,e)){let e={descr:l,section:i,number:i,width:150,padding:20,maxHeight:50};n.Rm.debug("eventNode",e);let c=t.append("g").attr("class","eventWrapper"),h=L.drawNode(c,e,i,a).height;o+=h,c.attr("transform",`translate(${s}, ${r})`),r=r+10+h}return r=l,o},"drawEvents"),H={setConf:(0,n.K2)(()=>{},"setConf"),draw:A},R=(0,n.K2)(t=>{let e="";for(let e=0;e<t.THEME_COLOR_LIMIT;e++)t["lineColor"+e]=t["lineColor"+e]||t["cScaleInv"+e],(0,r.A)(t["lineColor"+e])?t["lineColor"+e]=(0,a.A)(t["lineColor"+e],20):t["lineColor"+e]=(0,o.A)(t["lineColor"+e],20);for(let i=0;i<t.THEME_COLOR_LIMIT;i++){let n=""+(17-3*i);e+=`
    .section-${i-1} rect, .section-${i-1} path, .section-${i-1} circle, .section-${i-1} path  {
      fill: ${t["cScale"+i]};
    }
    .section-${i-1} text {
     fill: ${t["cScaleLabel"+i]};
    }
    .node-icon-${i-1} {
      font-size: 40px;
      color: ${t["cScaleLabel"+i]};
    }
    .section-edge-${i-1}{
      stroke: ${t["cScale"+i]};
    }
    .edge-depth-${i-1}{
      stroke-width: ${n};
    }
    .section-${i-1} line {
      stroke: ${t["cScaleInv"+i]} ;
      stroke-width: 3;
    }

    .lineWrapper line{
      stroke: ${t["cScaleLabel"+i]} ;
    }

    .disabled, .disabled circle, .disabled text {
      fill: lightgray;
    }
    .disabled text {
      fill: #efefef;
    }
    `}return e},"genSections"),M={db:c,renderer:H,parser:l,styles:(0,n.K2)(t=>`
  .edge {
    stroke-width: 3;
  }
  ${R(t)}
  .section-root rect, .section-root path, .section-root circle  {
    fill: ${t.git0};
  }
  .section-root text {
    fill: ${t.gitBranchLabel0};
  }
  .icon-container {
    height:100%;
    display: flex;
    justify-content: center;
    align-items: center;
  }
  .edge {
    fill: none;
  }
  .eventWrapper  {
   filter: brightness(120%);
  }
`,"getStyles")}}}]);
